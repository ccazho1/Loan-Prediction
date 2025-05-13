import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()  # Loads from .env into os.environ

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWSQL_USER"),
        password=os.getenv("SNOWSQL_PWD"),
        account=os.getenv("SNOWSQL_ACCOUNT"),
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA")
    )
    
    cur = conn.cursor()
    cur.execute("SELECT current_version();")
    version = cur.fetchone()
    print(f"Connected! Snowflake version: {version[0]}")

    cur.close()
    conn.close()


def main():
    get_snowflake_connection()

if __name__ == "__main__":
    main()
