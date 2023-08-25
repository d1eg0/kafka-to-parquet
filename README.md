# kafka-to-parquet

This is a tool to redirect all the records from a Kafka topic to parquet files.

Just a pet project to play with Rust :)

## How this works?

Spawns a thread per partition (using Tokio) to consume all the records from the lowest watermark to the highest one of the topic specified.

Each thread writes a batch of N records per parquet file adding an incremental suffix to the filename.

## To-do

Some features I'd like to add eventually:

- [ ] Decide a proper name
- [ ] Retrieve min max offsets from the topic (per partition) to set the consumer boundaries.
- [x] Kafka multi thread for multi partition topics.
- [ ] AVRO deserializer using schema registry
