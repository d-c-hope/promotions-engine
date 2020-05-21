from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
# from confluent_kafka.avro.serializer import SerializerError

string_serialiser = StringDeserializer('utf_8')

consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'group.id' : "groupid1234",
                 'key.deserializer': string_serialiser,
                 'value.deserializer': string_serialiser}

consumer = DeserializingConsumer(consumer_conf)


consumer.subscribe(['test-topic-profileus1'])

while True:
    try:
        msg = consumer.poll(10)
        print("poll returned {}".format(msg))

    except Exception as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print("Topic: {} Message val: {}".format(msg.topic(), msg.value()))

c.close()