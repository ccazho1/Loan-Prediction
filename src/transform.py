# Documentation Source: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from src import config
import numpy as np
from snowflake.connector.pandas_tools import write_pandas

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

    # deal with overscaled credit scores
    df["CREDIT_SCORE"] = df["CREDIT_SCORE"].apply(lambda x: x / 10 if x > 850 else x)

    # cases when both annual income and credit score are missing (21338 records)
    df["MISSING INCOME_AND_SCORE"] = (
        df["CREDIT_SCORE"].isnull() & df["ANNUAL_INCOME"].isnull()
    ).astype(int)
    median_credit = df["CREDIT_SCORE"].median()
    median_income = df["ANNUAL_INCOME"].median()

    df["CREDIT_SCORE"].fillna(median_credit, inplace=True)
    df["ANNUAL_INCOME"].fillna(median_income, inplace=True)
    
    

    df["CREDIT_SCORE_MISSING"] = df["CREDIT_SCORE"].isnull().astype(int)

    # deal with null annual income
    df["ANNUAL_INCOME_MISSING"] = df["ANNUAL_INCOME"].isnull().astype(int)
    df["ANNUAL_INCOME"] = df["ANNUAL_INCOME"].fillna(df["ANNUAL_INCOME"].median())
    df["LOG_ANNUAL_INCOME"] = np.log1p(df["ANNUAL_INCOME"])

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

    # deal with housing - standarizing categories for better modeling
    purpose_map = {
        "business loan": "business",
        "small_business": "business",
        "buy a car": "vehicle",
        "medical bills": "medical",
        "take a trip": "vacation",
        "vacation": "vacation",
        "major_purchase": "major_purchase",
        "educational expenses": "education",
        "buy house": "housing",
        "moving": "housing",
        "wedding": "wedding",
        "renewable_energy": "renewable_energy",
        "other": "other"
    }

    df["PURPOSE"] = df["PURPOSE"].str.strip().str.lower() # standardize casing and strip whitespace first
    df["PURPOSE"] = df["PURPOSE"].map(purpose_map) # groupinng purpose categories


    # one hot encoding columns for modeling: Home ownership, Term, Purpose
    df = pd.get_dummies(df, columns=["HOME_OWNERSHIP", "TERM", "PURPOSE"], drop_first=True)

def load_clean(df):
    conn = get_snowflake_connection()  # your existing function

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,                                 # your transformed dataframe
        table_name="loan_data_clean",         # desired table name
        schema=config.SNOWSQL_SCHEMA,         # optional if default schema set
        database=config.SNOWSQL_DATABASE,     # optional if default database set
        overwrite=True                        # optional: drop existing table
    )


if __name__ == "__main__":

    df = fetch_raw_data()
    transformation(df)
    load_clean(df)

