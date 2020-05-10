pushd ${MY_KAFKA}
./bin/zookeeper-server-stop.sh config/zookeeper.properties
./bin/kafka-server-stop.sh config/server.properties
popd