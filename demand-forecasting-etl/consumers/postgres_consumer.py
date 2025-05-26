import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

consumer_configuration = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'client.id': os.getenv('KAFKA_CLIENT_ID'),
    'group.id': os.getenv('KAFKA_GROUP_ID'),
    'auto.offset.reset': 'earliest'
}

pg_config = {
    'host': os.getenv('PG_HOST'),
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'port': os.getenv('PG_PORT')
}

consumer = Consumer(consumer_configuration)
logging.info(f"Starting consumer with {consumer_configuration}")

try:
    conn = psycopg2.connect(**pg_config)
    cursor = conn.cursor()
    logging.info("Connected to PostgreSQL database")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kafka_messages (
            id SERIAL PRIMARY KEY,
            topic VARCHAR(255),
            partition INTEGER,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
except Exception as e:
    logging.error(f"Error connecting to PostgreSQL: {e}")
    exit(1)

consumer.subscribe(['demo_topic'])
logging.info("Subscribed to demo_topic")

try:
    while True:
        msg = consumer.poll(1.0)
        time.sleep(2)

        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue
        message = msg.value().decode('utf-8')
        logging.info("decoded event")
        logging.info(f"Received from {msg.topic()}, part: {msg.partition()}")

        try:
            cursor.execute("""
                INSERT INTO kafka_messages (topic, partition, message)
                VALUES (%s, %s, %s)
            """, (msg.topic(), msg.partition(), message))
            conn.commit()
            logging.info("Message written to PostgreSQL")
        except Exception as e:
            logging.error(f"Error writing to PostgreSQL: {e}")
            conn.rollback()

except KeyboardInterrupt:
    logging.info("Consumer interrupted by user")
finally:
    consumer.close()
    cursor.close()
    conn.close()
    logging.info("Resources released and connections closed")
