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


def transform_and_merge():
    df_csv = pd.read_pickle("/tmp/df_csv.pkl")
    df_api = pd.read_pickle("/tmp/df_api.pkl")
    df_sql = pd.read_pickle("/tmp/df_sql.pkl")

    df_csv.rename(columns={"supplier_id": "supplier_uuid"}, inplace=True)
    df_sql.rename(columns={"supplier_id": "supplier_uuid"}, inplace=True)

    df_merged = df_csv.merge(df_api, on="supplier_uuid", how="outer") \
                      .merge(df_sql, on="supplier_uuid", how="outer")
    df_merged.fillna("N/A", inplace=True)

    output_path = f"/tmp/merged_supplier_data.csv"
    df_merged.to_csv(output_path, index=False)


def upload_to_s3():
    s3 = boto3.client("s3")
    bucket = "your-s3-bucket"
    key = "supplier_data/merged_supplier_data.csv"
    s3.upload_file("/tmp/merged_supplier_data.csv", bucket, key)
