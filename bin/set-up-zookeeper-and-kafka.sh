


docker run --net=host --rm confluentinc/cp-zookeeper:latest bash -c "echo stat | nc localhost 22181 | grep Mode"
docker run --net=host --rm confluentinc/cp-zookeeper:latest bash -c "echo stat | nc localhost 32181 | grep Mode"
docker run --net=host --rm confluentinc/cp-zookeeper:latest bash -c "echo stat | nc localhost 42181 | grep Mode"
