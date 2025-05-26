import json
import logging
import time

from confluent_kafka import Producer
from faker import Faker

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


producer_configuration = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'demo_producer',
    'acks': 'all',
    'compression.type': 'none',
    'retry.backoff.ms': 1000,
    'retry.backoff.max.ms': 5000,
    'message.timeout.ms': 10000,
    'retries': 5,
    'linger.ms': 100,
    'batch.num.messages': 1000
        }

producer = Producer(producer_configuration)

logging.info("Starting producer with config: %s", producer_configuration)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'At {time.strftime("%H:%M:%S", time.localtime())}, \
              your message was delivered to topic => {msg.topic()}, \
              partition => [{msg.partition()}], offset = {msg.offset()}')


sample_data = Faker()


i = 0
while i <= 100:
    event = {'index': i,
             'name': sample_data.name(),
             'phone_number': sample_data.phone_number(),
             'occupation': sample_data.job(),
             'country': sample_data.country(),
             'continent': sample_data.location_on_land()
             }
    i += 1
    time.sleep(2)
    logging.info(f"{event} successfully produced")
    serialize = json.dumps(event)
    logging.info("event serialised")
    producer.produce("demo_topic", serialize, callback=delivery_report)
    producer.produce("demo_topic2", serialize, callback=delivery_report)

logging.info("Flushing remaining messages...")
producer.flush()
logging.info("Producer shutdown complete")
