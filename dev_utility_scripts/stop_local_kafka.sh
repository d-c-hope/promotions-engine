pushd ${MY_KAFKA}
./bin/kafka-server-stop.sh config/server.properties
sleep 5s
./bin/zookeeper-server-stop.sh config/zookeeper.properties
popd
