
# Docker
# Docker Compose
* build, (re)create, start and attach to containers for a service

`docker-compose --file docker-compose.yml up -d`
* stop and remove containers

`docker-compose --file docker-compose.yml down`
* list containers

`docker-compose ps`
# Kafka administration

## Creating a topic
* execute bash on broker service

`docker-compose exec broker bash`
* create topic

`kafka-topics --create --topic name --bootstrap-server broker:9092 --replication-factor 1 --partitions 1`