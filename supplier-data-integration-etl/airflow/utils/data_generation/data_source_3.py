import logging
import random
import sqlite3

from faker import Faker

fake = Faker()
logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

conn = sqlite3.connect('../dags/suppliers.db')
cursor = conn.cursor()

logging.info("cursor connected")

cursor.execute("DROP TABLE IF EXISTS supplier_records")

cursor.execute('''
CREATE TABLE supplier_records (
    supplier_id TEXT PRIMARY KEY,
    category TEXT,
    rating REAL,
    last_order_date TEXT
)
''')

logging.info("db schema created")

for _ in range(1000):
    cursor.execute('''
    INSERT INTO supplier_records VALUES (?, ?, ?, ?)
    ''', (
        fake.uuid4(),
        random.choice(["Raw Materials", "Logistics", "Manufacturing"]),
        round(random.uniform(1.0, 5.0), 2),
        fake.date_between(start_date='-2y', end_date='today').isoformat()
    ))


conn.commit()
logging.info("connection commmitted")
conn.close()
logging.info("connection closed")
