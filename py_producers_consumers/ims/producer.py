from confluent_kafka import Producer

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import random
import time
from models import customer, gameStake

string_key_schema = avro.loads('{"type": "string"}')

avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=string_key_schema, default_value_schema=gameStake.game_value_schema)

sleepTime = 0.1

def produceGameEvent():
    for i in range(20):

        customerId = random.randint(0, 11)
        action = random.randint(1, 4)
        stake = random.randint(50, 100)

        key = "{}".format(customerId)
        value = {"game": "randomgame", "action": "action {}".format(action),
                 "customerID": "{}".format(customerId), "stake": stake}

        print("adding event")
#         avroProducer.produce(topic='test-topic-game1', value=value, key=key)
        avroProducer.produce(topic='test-topic-game1', value=value, key=key,
                key_schema=string_key_schema, value_schema=gameStake.game_value_schema)
        time.sleep(sleepTime)


firstNames = ["david", "andrew", "mark", "jason", "philippa","tommy", "harold",
                "guido", "martin", "kate", "anna", "ralph"]
lastNames = ["smith", "jones", "adams", "fielder", "thomas", "ferguson", "rossum",
                "george", "knapp", "taylor", "li", "heslop"]
# key_schema = avro.loads('{"type": "string"}')
def produceCustomerEvent():

    for i in range(12):

        customerId = i
#         firstNameIdx = random.randint(0, 6)
#         lastNameIdx = random.randint(0, 6)
        firstName = firstNames[i]
        lastName = lastNames[i]
        email = "{}.{}@mailinator.com".format(firstName,lastName)

        key = "{}".format(customerId)
        value = {"customerID": "{}".format(customerId), "firstName": "{}".format(firstName),
                 "email": "{}".format(email)}

        print("adding customer {}".format(value))
        avroProducer.produce(topic='test-topic-customer1', value=value, key=key, key_schema=string_key_schema,
        value_schema=customer.customer_value_schema)
        time.sleep(sleepTime)

produceCustomerEvent()
produceGameEvent()
print("finished producing")
avroProducer.flush()