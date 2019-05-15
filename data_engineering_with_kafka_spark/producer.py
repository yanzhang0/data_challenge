""" Generate test data to send to Kafka """

import random
from time import sleep
from json import dumps
import json

from kafka import KafkaProducer, KafkaClient

# Import test data

prefix='./events/'

with open(prefix+'applications.json') as json_file:  
    data = json.load(json_file)


# Set up producer

KAFKA = KafkaClient('localhost:9092')
PRODUCER = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='test-producer'
)

TOPIC = 'test-topic'


# Loop, add to kafka


i=0
for tdata in data:


    try:
        #avro_push(rec)
        PRODUCER.send(TOPIC, json.dumps(tdata).encode('utf-8'))
    except UnicodeDecodeError:
        pass
    i+=1
    print('pushed: %d' % i)

    # Frequency, note that the total number of calls is constant, 
    # Therefore we need to only use one parameter 
    sleep(0.05)
