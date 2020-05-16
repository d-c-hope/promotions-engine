pushd ${MY_KAFKA}
./bin/zookeeper-server-stop.sh config/zookeeper.properties
sleep 5s
./bin/kafka-server-stop.sh config/server.properties
popd