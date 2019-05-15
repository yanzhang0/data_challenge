# Read result from Kafka

import json

from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('result',
                         group_id='result',
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    print ("%s:%d:%d: key=%s val=%s " % (message.topic, message.partition,
                                         message.offset, message.key, message.value))

# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))


# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)
