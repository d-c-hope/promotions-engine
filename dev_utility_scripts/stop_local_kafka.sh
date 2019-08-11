pushd /usr/local/kafka_2.12-2.3.0
./bin/zookeeper-server-stop.sh config/zookeeper.properties
./bin/kafka-server-stop.sh config/server.properties
popd