
create:
	docker-compose exec kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic test --partitions 2 --replication-factor 1"

delete:
	docker-compose exec kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --delete --topic test"
	
list:
	docker-compose exec kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --list"
	