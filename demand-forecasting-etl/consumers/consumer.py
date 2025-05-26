import logging
import time
import json
import pandas as pd
from confluent_kafka import Consumer

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

consumer_configuration = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'stream_consumer',
    'group.id': 'consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_configuration)

logging.info(f"Starting consumer with {consumer_configuration}")

consumer.subscribe(['demand_forecast'])

logging.info("Subscribed to demand_forecast")

while True:
    msg = consumer.poll(1.0)
    time.sleep(2)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    message = msg.value().decode('utf-8')
    message_data = json.loads(message)
    transformed_value = pd.json_normalize(message_data)
    logging.info("decoded event")
    logging.info(f"Received message {transformed_value} from topic => {msg.topic()}, \
          partition => {msg.partition()}")
    print(transformed_value.to_string(index=False))

consumer.close()
logging.info("Connection closed unexpectedly")
