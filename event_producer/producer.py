from confluent_kafka import Producer

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import random
import time
from models import customer, gameStake



avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=gameStake.game_key_schema, default_value_schema=gameStake.game_value_schema)

sleepTime = 0.1
def produceGameEvent():
    for i in range(20):

        customerId = random.randint(1, 10)
        action = random.randint(1, 4)
        stake = random.randint(50, 100)

        value = {"game": "randomgame", "action": "action {}".format(action),
                 "customerID": "{}".format(customerId), "stake": stake}
        key = {"customerID": "{}".format(customerId)}

        print("adding event")
        avroProducer.produce(topic='test-topic-game1', value=value, key=key)
        time.sleep(sleepTime)


firstNames = ["david", "andrew", "mark", "jason", "tommy", "harold", "guido"]
lastNames = ["smith", "jones", "moses", "fielder", "thomas", "ferguson", "rossum"]
key_schema = avro.loads('{"type": "string"}')
def produceCustomerEvent():

    for i in range(22):

        customerId = i % 8
        firstNameIdx = random.randint(0, 6)
        lastNameIdx = random.randint(0, 6)
        firstName = firstNames[firstNameIdx]
        lastName = lastNames[lastNameIdx]
        email = "{}.{}@mailinator.com".format(firstName,lastName)

        key = "{}".format(customerId)
        value = {"customerID": "{}".format(customerId), "firstName": "{}".format(firstName),
                 "email": "{}".format(email)}

        print("adding customer {}".format(value))
        avroProducer.produce(topic='test-topic-customer1', value=value, key=key, key_schema=key_schema, value_schema=customer.customer_value_schema)
        time.sleep(sleepTime)

produceCustomerEvent()
# produceGameEvent()

avroProducer.flush()