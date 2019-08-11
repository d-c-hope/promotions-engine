pushd /usr/local/kafka_2.12-2.3.0
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
popd