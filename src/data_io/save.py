import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from src.data_io.fetch import get_snowflake_connection
from src import config

def save_to_csv(df: pd.DataFrame, path='data/clean/loan_prediction_info.csv'):
    df.to_csv(path, index=False)
    print(f"[INFO] Saved to {path}")

def save_to_snowflake(df: pd.DataFrame, table_name="LOAN_DATA_CLEAN"):
    conn = get_snowflake_connection()
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        schema=config.SNOWSQL_SCHEMA,
        database=config.SNOWSQL_DATABASE,
        overwrite=True,
        auto_create_table=True # turn to true if modifying row names
    )
    print(f"[INFO] Uploaded {nrows} rows to Snowflake.")
