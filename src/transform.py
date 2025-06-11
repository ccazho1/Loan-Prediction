# Documentation Source: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from src import config
from snowflake.connector.pandas_tools import write_pandas
import src.features
from src.feature_builder import fb

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


def fetch_raw_data():
    print("[INFO] Pulling raw loan_data from Snowflake...")
    conn = get_snowflake_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {config.TABLE_NAME};")
        df = cursor.fetch_pandas_all() # uses Apache Arrow under the hood, efficient for large datasets
        print(f"[INFO] Retrieved {len(df)} rows.")
        return df
    finally:
        # safe cleanup!
        cursor.close()
        conn.close()

def transformation(df: pd.DataFrame) -> pd.DataFrame:
    # remove symbols to convert to float (monthly debt column)
    df["MONTHLY_DEBT"] = (
        df["MONTHLY_DEBT"]
        .str.replace("[$,]", "", regex=True)
        .astype(float)
    )
    df["MONTHLY_DEBT"] = df["MONTHLY_DEBT"].astype(float)
    df["MONTHLY_DEBT"] = df["MONTHLY_DEBT"].fillna(df["MONTHLY_DEBT"].median())

    # deal with overscaled credit scores
    df["CREDIT_SCORE"] = df["CREDIT_SCORE"].apply(lambda x: x / 10 if x > 850 else x)

    df["CREDIT_SCORE_MISSING"] = df["CREDIT_SCORE"].isnull().astype(int)


    # deal with null annual income
    df["ANNUAL_INCOME_MISSING"] = df["ANNUAL_INCOME"].isnull().astype(int)


    # cases when both annual income and credit score are missing (21338 records)
    df["MISSING_INCOME_AND_CREDIT_SCORE"] = (
        df["CREDIT_SCORE"].isnull() & df["ANNUAL_INCOME"].isnull()
    ).astype(int)
    
    # fill missing values with median
    median_credit = df["CREDIT_SCORE"].median()
    median_income = df["ANNUAL_INCOME"].median()
    df["ANNUAL_INCOME"].fillna(median_income, inplace=True)
    df["CREDIT_SCORE"].fillna(median_credit, inplace=True)

    # deal with years_in_job
    df["YEARS_IN_JOB"] = pd.to_numeric(df["YEARS_IN_JOB"], errors="coerce")
 
    # deal with loan_status
    loan_status_map = {
        "Fully Paid": 0,
        "Charged Off": 1
    }
    df["LOAN_STATUS"] = df["LOAN_STATUS"].map(loan_status_map)

    # deal with current_loan_amount
    # Flag them
    df["LOAN_AMOUNT_PLACEHOLDER"] = (df["CURRENT_LOAN_AMOUNT"] == 99999999).astype(int)
    # Impute
    median_val = df[df["CURRENT_LOAN_AMOUNT"] != 99999999]["CURRENT_LOAN_AMOUNT"].median()
    df["CURRENT_LOAN_AMOUNT"] = df["CURRENT_LOAN_AMOUNT"].replace(99999999, median_val)

    # standarizing categories for better modeling

    return df

def load_clean(df):
    # save to data/clean
    df.to_csv('data/clean/loan_prediction_info.csv', index='False')

    # upload clean dataset snowflake
    conn = get_snowflake_connection()
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,                                # transformed dataframe
        table_name="LOAN_DATA_CLEAN",         # table name
        schema=config.SNOWSQL_SCHEMA,         # schema set
        database=config.SNOWSQL_DATABASE,     # database set
        overwrite=True,                       
        auto_create_table=False               # enable when change column names       
    )

if __name__ == "__main__":

    df = fetch_raw_data()
    df = transformation(df)
    df = fb.run(df)
    load_clean(df)

