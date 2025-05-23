import pandas as pd
import json
import sqlite3
from datetime import datetime
import boto3

def extract_csv():
    df = pd.read_csv("../mock_supplier_data.csv")
    df.to_pickle("/tmp/df_csv.pkl")


def extract_api():
    with open("../api_suppliers.json") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df.to_pickle("/tmp/df_api.pkl")


def extract_sql():
    conn = sqlite3.connect("../suppliers.db")
    df = pd.read_sql_query("SELECT * FROM supplier_records", conn)
    conn.close()
    df.to_pickle("/tmp/df_sql.pkl")


