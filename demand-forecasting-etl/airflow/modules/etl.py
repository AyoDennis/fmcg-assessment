import pandas as pd
import json
import sqlite3
from datetime import datetime
import boto3

def extract_csv():
    df = pd.read_csv("../mock_supplier_data.csv")
    df.to_pickle("/tmp/df_csv.pkl")


