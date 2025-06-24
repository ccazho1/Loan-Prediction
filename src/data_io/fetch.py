# Documentation Source: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
import os
from src import config
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWSQL_USER"),
        password=os.getenv("SNOWSQL_PWD"),
        account=os.getenv("SNOWSQL_ACCOUNT"),
        warehouse=os.getenv("SNOWSQL_WAREHOUSE"),
        database=os.getenv("SNOWSQL_DATABASE"),
        schema=os.getenv("SNOWSQL_SCHEMA")
    )


def fetch_raw_data(table_name: str = config.TABLE_NAME):
    print("[INFO] Pulling raw loan_data from Snowflake...")
    conn = get_snowflake_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name};")
        df = cursor.fetch_pandas_all() # uses Apache Arrow under the hood, efficient for large datasets
        print(f"[INFO] Retrieved {len(df)} rows.")
        return df
    finally:
        # safe cleanup!
        cursor.close()
        conn.close()
