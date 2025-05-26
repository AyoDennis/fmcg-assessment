import logging
import time
import pandas as pd
import json
import boto3
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
import os
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

class RedshiftStreamer:
    def __init__(self, kafka_config: Dict, redshift_config: Dict, batch_size: int = 100):
        self.kafka_config = kafka_config
        self.redshift_config = redshift_config
        self.batch_size = batch_size
        self.batch_data = []
        self.consumer = None
        self.redshift_conn = None
        
    def setup_kafka_consumer(self):
        """Initialize Kafka consumer"""
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe(['demand_forecast'])
        logger.info("Kafka consumer initialized and subscribed to demand_forecast")
        
    def setup_redshift_connection(self):
        """Initialize Redshift connection"""
        try:
            self.redshift_conn = psycopg2.connect(
                host=self.redshift_config['host'],
                port=self.redshift_config['port'],
                dbname=self.redshift_config['database'],
                user=self.redshift_config['user'],
                password=self.redshift_config['password']
            )
            logger.info("Connected to Redshift successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {e}")
            raise
            
    def create_sales_table(self):
        """Create the sales table in Redshift if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS demand_forecast (
            sale_id VARCHAR(50) PRIMARY KEY,
            channel VARCHAR(20) NOT NULL,
            customer_id VARCHAR(50) NOT NULL,
            customer_name VARCHAR(50) NOT NULL,
            customer_email VARCHAR(100),
            product_category VARCHAR(50),
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(10,2),
            payment_method VARCHAR(50),
            timestamp BIGINT,
            event_datetime TIMESTAMP,
            country VARCHAR(10),
            city VARCHAR(100),
            postcode VARCHAR(20),
            created_at TIMESTAMP DEFAULT GETDATE()
        )
        DISTKEY(channel)
        SORTKEY(timestamp);
        """
        
        try:
            with self.redshift_conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.redshift_conn.commit()
                logger.info("Sales table created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
            
    def process_message(self, message_data: Dict) -> Dict:
        """Process and transform message data"""
        # Convert timestamp to datetime
        event_datetime = datetime.fromtimestamp(message_data['timestamp'] / 1000)
        
        processed_data = {
            'sale_id': str(message_data['sale_id']),
            'channel': message_data['channel'],
            'customer_id': str(message_data['customer_id']),
            'customer_name': message_data['customer_name'],
            'customer_email': message_data['customer_email'],
            'product_category': message_data['product_category'],
            'quantity': message_data['quantity'],
            'unit_price': message_data['unit_price'],
            'total_price': message_data['total_price'],
            'payment_method': message_data['payment_method'],
            'timestamp': message_data['timestamp'],
            'event_datetime': event_datetime,
            'country': message_data['location']['country'],
            'city': message_data['location']['city'],
            'postcode': message_data['location']['postcode']

        }
        
        return processed_data
        
    def batch_insert_to_redshift(self, batch_data: List[Dict]):
        """Insert batch data to Redshift"""
        if not batch_data:
            return
            
        insert_query = """
        INSERT INTO demand_forecast (
            sale_id, channel, customer_id, customer_name, customer_email,
            product_category, quantity, unit_price, total_price,
            payment_method, timestamp, event_datetime, country, city, postcode
        ) VALUES (
            %(sale_id)s, %(channel)s, %(customer_id)s, %(customer_name)s, %(customer_email)s,
            %(product_category)s, %(quantity)s, %(unit_price)s, %(total_price)s,
            %(payment_method)s, %(timestamp)s, %(event_datetime)s, %(country)s, %(city)s, %(postcode)s
        )
        """
        
        try:
            with self.redshift_conn.cursor() as cursor:
                cursor.executemany(insert_query, batch_data)
                self.redshift_conn.commit()
                logger.info(f"Successfully inserted {len(batch_data)} records to Redshift")
        except Exception as e:
            logger.error(f"Failed to insert batch to Redshift: {e}")
            self.redshift_conn.rollback()
            raise
            
    def stream_to_redshift(self):
        """Main streaming loop"""
        self.setup_kafka_consumer()
        self.setup_redshift_connection()
        self.create_sales_table()
        
        logger.info("Starting streaming pipeline...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    # No message, check if we should flush batch
                    if self.batch_data:
                        logger.info("Flushing partial batch due to timeout")
                        self.batch_insert_to_redshift(self.batch_data)
                        self.batch_data.clear()
                    continue
                    
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    # Process message
                    message_str = msg.value().decode('utf-8')
                    message_data = json.loads(message_str)
                    processed_data = self.process_message(message_data)
                    
                    # Add to batch
                    self.batch_data.append(processed_data)
                    logger.info(f"Added record to batch. Batch size: {len(self.batch_data)}")
                    
                    # Insert batch if size reached
                    if len(self.batch_data) >= self.batch_size:
                        self.batch_insert_to_redshift(self.batch_data)
                        self.batch_data.clear()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode JSON: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
            
        finally:
            # Insert any remaining data
            if self.batch_data:
                self.batch_insert_to_redshift(self.batch_data)
                
            if self.consumer:
                self.consumer.close()
            if self.redshift_conn:
                self.redshift_conn.close()
            logger.info("Streaming pipeline closed")

def main():
    # Configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'redshift_stream_consumer',
        'group.id': 'redshift_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    
    redshift_config = {
        'host': os.getenv('REDSHIFT_HOST'),
        'port': int(os.getenv('REDSHIFT_PORT')), #original was 'port': int(os.getenv('REDSHIFT_PORT'))
        'database': os.getenv('REDSHIFT_DB'),
        'user': os.getenv('REDSHIFT_USER'),
        'password': os.getenv('REDSHIFT_PASSWORD')
    }
    
    # Initialize and start streamer
    streamer = RedshiftStreamer(kafka_config, redshift_config, batch_size=50)
    streamer.stream_to_redshift()

if __name__ == "__main__":
    main()