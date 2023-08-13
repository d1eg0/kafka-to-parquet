use std::ptr::null;
use std::time::Duration;

use clap::{App, Arg};
use env_logger;
use log::{info, warn};

use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::writer::SerializedColumnWriter;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;

use std::{fs, path::Path, sync::Arc};

use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn produce(brokers: &str, topic: &str, n_messages: usize) {
    info!("Producer on");
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..n_messages)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

struct ParquetWritter {
    path: String,
    msg: Vec<String>
}

impl ParquetWritter {
    fn write_to_parquet(self) {
        let mut values = Vec::new();
        for m in self.msg {
            let v = ByteArray::from(m.as_str());
            values.push(v)
        }
        let path = Path::new(&self.path);
    
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY b (UTF8);
            }
            ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let file = fs::File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
    
    
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            // ... write values to a column writer
            match col_writer.untyped() {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) =>  {
                    //let a = "hola";
                    typed.write_batch(values.as_slice(), None, None);
                }
                _ => {
                    unimplemented!();
                }
            
            }
            col_writer.close().unwrap();
        }
        row_group_writer.close().unwrap();
        writer.close().unwrap();
    }
}



async fn consume_and_print(brokers: &str, group_id: &str, topic: &str, batch_size: usize) {
    let mut msg_i = 0;
    let mut batch_id: u32 = 0;
    let mut msg_batch = Vec::with_capacity(batch_size);
    let topics: [&str; 1] = [topic];
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let timeout = Duration::new(5,0);
    let m = consumer.fetch_metadata::<Duration>(Some(topic), timeout );
    match m {
        Ok(m1) => {
            for tm in m1.topics() {
                println!("Topic: '{:?}'. Number of partitions: {:?}",tm.name(), tm.partitions().len())

            }
    },
        Err(e) => {
            warn!("Error getting topic metadata: {:?}", e);
        }
    }

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");


  
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                //msg_i=msg_i + 1;
                let payload: &str = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
               
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                msg_batch.push(payload.to_string().clone());
                msg_i = msg_i + 1;
                if msg_i == batch_size {
                    let writer = ParquetWritter {path: String::from("batch_") + &batch_id.to_string() + &".parquet".to_string(), msg: msg_batch.to_owned()};
                    writer.write_to_parquet();
                    msg_batch.clear();
                    msg_i = 0;
                    batch_id+=1;
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topic")
                .short("t")
                .long("topic")
                .help("Topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("mode")
                .short("m")
                .long("mode")
                .help("mode: C consumer or P producer")
                .takes_value(true)
                .default_value("C"),
        ).arg(
            Arg::with_name("num-messages")
                .short("n")
                .long("num-messages")
                .help("number of messages produced or consumed")
                .takes_value(true)
                .default_value("10"),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let num_messages = matches.value_of("num-messages").unwrap_or("10").parse::<usize>().unwrap();

    match matches.value_of("mode") {
        Some("p") => produce(brokers, &topic, num_messages).await,
        Some(_) | None => consume_and_print(brokers, group_id, &topic, num_messages).await,
    }
}
