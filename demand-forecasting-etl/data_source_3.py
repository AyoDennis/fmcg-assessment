import sqlite3
import json
import random
import logging
from data_source_1 import fake

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

conn = sqlite3.connect('./demand-forecasting-etl/suppliers.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE supplier_records (
    supplier_id TEXT PRIMARY KEY,
    category TEXT,
    rating REAL,
    last_order_date TEXT
)
''')

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
conn.close()
