import sqlite3
import json
import random
import logging
from data_source_1 import fake

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

conn = sqlite3.connect('suppliers.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE supplier_records (
    supplier_id TEXT PRIMARY KEY,
    category TEXT,
    rating REAL,
    last_order_date TEXT
)
''')

