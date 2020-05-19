
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

import random
import time
from uuid import uuid4
import json

string_serialiser = StringSerializer('utf_8')

producer_conf = {'bootstrap.servers': 'localhost:9092',
                 'key.serializer': string_serialiser,
                 'value.serializer': string_serialiser}

producer = SerializingProducer(producer_conf)

sleepTime = 0.1

firstNames = ["david", "andrew", "mark", "jason", "philippa","tommy", "harold",
              "guido", "martin", "kate", "anna", "ralph"]
lastNames = ["smith", "jones", "adams", "fielder", "thomas", "ferguson", "rossum",
             "george", "knapp", "taylor", "li", "heslop"]

def produceProfileCreatedEvent():
    for i in range(20):

        profileId = str(uuid4())
        firstNameIdx = random.randint(0, 11)
        lastNameIdx = random.randint(0, 11)
        firstName = firstNames[firstNameIdx]
        lastName = lastNames[lastNameIdx]

        key = "{}".format(profileId)
        value = {
                    "profileId": profileId,
                    "firstName":firstName,
                    "lastName":lastName
                }
        valueStr = json.dumps(value)
        print("adding event")

        producer.produce(topic="test-topic-createprofile1", key=key, value=valueStr)

        time.sleep(sleepTime)

produceProfileCreatedEvent()
print("finished producing")
producer.flush()