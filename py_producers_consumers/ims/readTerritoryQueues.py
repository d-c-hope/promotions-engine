from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer


consumer_conf = {'bootstrap.servers': 'localhost:9092',
                 'key.serializer': string_serialiser,
                 'value.serializer': string_serialiser}

consumer = SerializingConsumer(consumer_conf)



consumer.subscribe(['test-topic-profileus1'])

while True:
    try:
        msg = c.poll(10)
        print("poll returned {}".format(msg))

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print("Topic: {} Message val: {}".format(msg.topic(), msg.value()))

c.close()