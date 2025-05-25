from dotenv import load_dotenv
import os
import pandas as pd
import json
import sqlite3
from datetime import datetime
import boto3
import logging
from airflow.models import Variable
from botocore.exceptions import ClientError

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

load_dotenv()

def extract_csv():
    """
    Converts csv data source into a pickle binary object
    """
    df = pd.read_csv("./mock_supplier_data.csv")
    logging.info("csv read")
    df.to_pickle("./tmp/df_csv.pkl")

logging.info("csv data source pickled")

def extract_api():
    """
    Converts api data source into a pickle binary object
    """
    with open("./api_suppliers.json") as f:
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
    conn = sqlite3.connect("./suppliers.db")
    logging.info("connected to db source")
    df = pd.read_sql_query("SELECT * FROM supplier_records", conn)
    logging.info("queried db source")
    conn.close()
    logging.info("closed db connection")
    df.to_pickle("./tmp/df_sql.pkl")

logging.info("db source pickled")

def transform_and_merge():
    """
    Loads pickled DataFrames, merges side-by-side, renames columns, then saves to CSV.
    """
    
    df_csv = pd.read_pickle("./tmp/df_csv.pkl")
    logging.info("Pickled CSV data converted into DataFrame")
    df_api = pd.read_pickle("./tmp/df_api.pkl")
    logging.info("Pickled API data converted into DataFrame")
    df_sql = pd.read_pickle("./tmp/df_sql.pkl")
    logging.info("Pickled SQL data converted into DataFrame")

    df_merged = pd.concat([
        df_csv.reset_index(drop=True),
        df_api.reset_index(drop=True),
        df_sql.reset_index(drop=True)
    ], axis=1)

    logging.info("DataFrames merged side-by-side")

    # Rename columns AFTER merging
    df_merged.rename(columns={
        "supplier_id": "supplier_uuid",  # Rename if present in any DataFrame
        "supplier_id_x": "supplier_uuid",  # Handle possible merge suffixes
        "supplier_id_y": "supplier_uuid"
    }, inplace=True)

    logging.info("Column names standardized post-merge")

    # Fill NaN (if any) and save
    df_merged.fillna("N/A", inplace=True)
    output_path = "./merged_supplier_data.csv"
    df_merged.to_csv(output_path, index=False)
    logging.info(f"Merged data saved to {output_path}")


def aws_session():
    session = boto3.Session(
                    aws_access_key_id=Variable.get('access_key'),
                    aws_secret_access_key=Variable.get('secret_key'),
                    region_name="eu-central-1"
    )
    return session


def boto3_client(aws_service):

    client = boto3.client(aws_service,
                          aws_access_key_id=Variable.get('access_key'),
                          aws_secret_access_key=Variable.get('secret_key'),
                          region_name="eu-central-1")

    return client

def upload_to_s3():
    """
    Uploads the merged CSV file to S3 with proper error handling and logging.
    """
    try:
        s3 = boto3_client("s3")
        bucket = "fmcg-de-assessment"
        key = "supplier_data/merged_supplier_data.csv"
        local_path = "./merged_supplier_data.csv"
        
        # Verify file exists before upload
        import os
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        # Perform upload
        s3.upload_file(local_path, bucket, key)
        
        # Verify upload succeeded
        s3.head_object(Bucket=bucket, Key=key)
        logging.info(f"Successfully uploaded to s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logging.error(f"S3 upload failed: {str(e)}")
        if e.response['Error']['Code'] == '404':
            logging.error("The file does not exist or permission denied")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during S3 upload: {str(e)}")
        raise


try:
    logging.info("Starting ETL pipeline")
    
    extract_csv()
    extract_api()
    extract_sql()
    transform_and_merge()
    upload_to_s3()
    
   
    logging.info("ETL completed successfully")
except Exception as e:
    logging.error(f"ETL failed: {str(e)}")