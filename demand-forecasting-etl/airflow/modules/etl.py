import pandas as pd
import json
import sqlite3
from datetime import datetime
import boto3
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

def extract_csv():
    """
    Converts csv data source into a pickle binary object
    """
    df = pd.read_csv("../mock_supplier_data.csv")
    logging.info("csv read")
    df.to_pickle("./tmp/df_csv.pkl")

logging.info("csv data source pickled")

def extract_api():
    """
    Converts api data source into a pickle binary object
    """
    with open("../api_suppliers.json") as f:
        data = json.load(f)
    logging.info("api data read")
    df = pd.json_normalize(data)
    logging.info("api json data normalized")
    df.to_pickle("./tmp/df_api.pkl")

logging.info("api data source pickled")

def extract_sql():
    """
    Converts database source into a pickle binary object
    """
    conn = sqlite3.connect("../suppliers.db")
    logging.info("connected to db source")
    df = pd.read_sql_query("SELECT * FROM supplier_records", conn)
    logging.info("queried db source")
    conn.close()
    logging.info("closed db connection")
    df.to_pickle("./tmp/df_sql.pkl")

logging.info("db source pickled")

def transform_and_merge():
    """
    Converts all pickled data sources into daaframes
    """
    df_csv = pd.read_pickle("./tmp/df_csv.pkl")
    logging.info("pickled csv converted into dataframe")
    df_api = pd.read_pickle("./tmp/df_api.pkl")
    logging.info("pickled api data converted into dataframe")
    df_sql = pd.read_pickle("./tmp/df_sql.pkl")
    logging.info("pickled db data converted into dataframe")

    df_csv.rename(columns={"supplier_id": "supplier_uuid"}, inplace=True)
    logging.info("csv df columns names transformed for consistency")
    df_sql.rename(columns={"supplier_id": "supplier_uuid"}, inplace=True)
    logging.info("sql df columns names transformed for consistency")

    df_merged = df_csv.merge(df_api, on="supplier_uuid", how="outer") \
                      .merge(df_sql, on="supplier_uuid", how="outer")
    df_merged.fillna("N/A", inplace=True)
    logging.info("csv, db and api columns names merged for consistency")

    output_path = f"./merged_supplier_data.csv"
    df_merged.to_csv(output_path, index=False)
    logging.info("all data sources successfully merged")


# def upload_to_s3():
#     s3 = boto3.client("s3")
#     bucket = "fmcg-assessment"
#     key = "supplier_data/merged_supplier_data.csv"
#     s3.upload_file("/tmp/merged_supplier_data.csv", bucket, key)
