use std::path::PathBuf;
use std::ptr::null;
use std::thread::JoinHandle;
use std::time::Duration;

use clap::{App, Arg};
use env_logger;
use log::{info, warn, error};

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

use std::{thread, time};

use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

const BASE_PATH: &str = "output";

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
    path: PathBuf,
    msg: Vec<String>,
}

impl ParquetWritter {
    fn write_to_parquet(self) {
        let mut values = Vec::new();
        for m in self.msg {
            let v = ByteArray::from(m.as_str());
            values.push(v)
        }
        //let path = Path::new(&self.path);

        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY b (UTF8);
            }
            ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let file = fs::File::create(self.path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            // ... write values to a column writer
            match col_writer.untyped() {
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
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

#[derive(Debug)]
struct TopicProperties {
    partitions: usize,
    watermarks: Vec<(i64, i64)>,
}

#[derive(Debug)]
struct KafkaConfig {
    brokers: String,
    group_id: String,
    batch_size: usize,
    topic: String,
    properties: TopicProperties,
}

fn load_topic_properties(brokers: &str, group_id: &str, topic: &str) -> TopicProperties {
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

    let topic_properties = get_topic_properties(&consumer, topic);
    println!(
        "partitions:{:?} watermarks:{:?}",
        topic_properties.partitions, topic_properties.watermarks
    );
    topic_properties
}

fn get_topic_properties(consumer: &LoggingConsumer, topic: &str) -> TopicProperties {
    // Get number of partitions
    let timeout = Duration::new(5, 0);
    let m = consumer.fetch_metadata::<Duration>(Some(topic), timeout);
    let num_partitions = match m {
        Ok(m1) => m1.topics().first().unwrap().partitions().len(),
        Err(e) => {
            warn!("Error getting topic metadata: {:?}", e);
            0
        }
    };

    let mut watermarks: Vec<(i64, i64)> = Vec::with_capacity(num_partitions);
    for p in 0..num_partitions {
        let w = consumer.fetch_watermarks(topic, 0, timeout);
        watermarks.push(w.unwrap());
    }

    TopicProperties {
        partitions: num_partitions,
        watermarks: watermarks,
    }
}

fn create_consumer(kafka_config: &KafkaConfig, partition: i32) -> LoggingConsumer {
    info!(
        "Starting consumer for partition {:?}",
        partition.to_string()
    );
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &kafka_config.group_id)
        .set("bootstrap.servers", &kafka_config.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let mut assignment: TopicPartitionList = TopicPartitionList::with_capacity(1);
    assignment.add_partition(&kafka_config.topic, partition);
    consumer
        .assign(&assignment)
        .expect("Can't subscribe to specified topics");

    consumer
}

async fn consume_and_write(consumer: LoggingConsumer, batch_size: usize) {
    let mut msg_i = 0;
    let mut batch_id: u32 = 0;
    let mut msg_batch = Vec::with_capacity(batch_size);

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
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
                    let writer = ParquetWritter {
                        path: get_parquet_path(m.partition(), batch_id),
                        msg: msg_batch.to_owned(),
                    };
                    writer.write_to_parquet();
                    msg_batch.clear();
                    msg_i = 0;
                    batch_id += 1;
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

fn get_parquet_path(partition: i32, batch_id: u32) -> PathBuf {
    let filename = String::from("partition_")
        + &partition.to_string()
        + &"_batch_".to_string()
        + &batch_id.to_string()
        + &".parquet".to_string();
    let mut path =  PathBuf::from(BASE_PATH);
    path.push(filename);
    path
}

fn init_output() {
    let path =  PathBuf::from(BASE_PATH);
    match std::fs::create_dir(&path) {
        Ok(_) => (),
        Err(error) => {
            error!("Error creating output path {:?}", error);
            panic!("Check if the path '{:?}' already exists", &path);
        }
    }
}

async fn run_pipeline(kafka_config: Arc<KafkaConfig>) {
    let partitions = kafka_config.properties.partitions.clone();
    let mut handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let kafka_config = kafka_config.clone();
        let handler = tokio::spawn(async move {
            let consumer = create_consumer(&kafka_config, partition as i32);
            consume_and_write(consumer, kafka_config.batch_size).await;
        });
        handlers.push(handler);
    }
    for h in handlers {
        let _ = h.await;
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
        )
        .arg(
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
    let num_messages = matches
        .value_of("num-messages")
        .unwrap_or("10")
        .parse::<usize>()
        .unwrap();

    match matches.value_of("mode") {
        Some("p") => produce(brokers, &topic, num_messages).await,
        Some(_) | None => {
            init_output();
            let topic_properties = load_topic_properties(brokers, group_id, &topic);
            let kafka_config = Arc::new(KafkaConfig {
                topic: topic.to_owned(),
                brokers: brokers.to_owned(),
                group_id: group_id.to_owned(),
                batch_size: num_messages,
                properties: topic_properties,
            });
            let _ = run_pipeline(kafka_config).await;
        }
    }
}
