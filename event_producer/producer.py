from confluent_kafka import Producer

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import random


value_schema_str = """
{
   "namespace": "cch.game.event",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "game",
       "type" : "string"
     },
     {
       "name" : "action",
       "type" : "string"
     },
     {
       "name" : "customerID",
       "type" : "string"
     },
     {
       "name" : "stake",
       "type" : "int"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "cch.game.event",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "customerID",
       "type" : "string"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)

avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

for i in range(200):

    customerId = random.randint(1, 10)¶
    action = random.randint(1, 4)¶
    stake = random.randint(50, 100)¶


    # value = {"game": "randomgame", "action": "action 4", "customerID": "{}".format(customerId), "stake": 23 }
    # key = {"customerID": "23416"}

    value = {"game": "randomgame", "action": "action {}".format(action),
             "customerID": "{}".format(customerId), "stake": stake}
    key = {"customerID": customerId}


    print("adding event")
    avroProducer.produce(topic='test-topic-1', value=value, key=key)
    time.sleep(0.5)
avroProducer.flush()