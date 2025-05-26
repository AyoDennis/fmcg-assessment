import json
import logging
import time
import random
from faker import Faker

from confluent_kafka import Producer

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


producer_configuration = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'stream producer',
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


from faker import Faker
import random
import time

sample_data = Faker()

CHANNELS = {
    "web": {"product_categories": ["electronics", "books", "clothing", "home"], "price_range": (10, 1000)},
    "mobile": {"product_categories": ["apps", "games", "subscriptions", "in-app"], "price_range": (1, 200)},
    "store": {"product_categories": ["groceries", "furniture", "appliances", "toys"], "price_range": (5, 5000)}
}

i = 0
while i <= 100:
    channel_name = random.choice(list(CHANNELS.keys()))
    channel_config = CHANNELS[channel_name]
    product_category = random.choice(channel_config["product_categories"])
    min_price, max_price = channel_config["price_range"]
    
    event = {
        "sale_id": sample_data.uuid4(),
        "channel": channel_name,
        "customer_id": sample_data.uuid4(),
        "customer_name": sample_data.name(),
        "customer_email": sample_data.email(),
        "product_name": sample_data.catch_phrase(),
        "product_category": product_category,
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(min_price, max_price), 2),
        "total_price": 0,
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash"]),
        "timestamp": int(time.time() * 1000),
        "location": {
            "country": sample_data.country_code(),
            "city": sample_data.city(),
            "postcode": sample_data.postcode()
        }
    }
    
    event["total_price"] = round(event["quantity"] * event["unit_price"], 2)

    i += 1
    time.sleep(2)
    logging.info(f"{event} successfully produced")
    serialize = json.dumps(event)
    logging.info("event serialised")
    producer.produce("demand_forecast", serialize, callback=delivery_report)

logging.info("Flushing remaining messages...")
producer.flush()
logging.info("Producer shutdown complete")

