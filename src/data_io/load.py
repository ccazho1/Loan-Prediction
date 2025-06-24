import os
import subprocess
from src import config


CREATE_TABLE_SQL = f"""
CREATE WAREHOUSE IF NOT EXISTS {config.SNOWSQL_WAREHOUSE};
CREATE DATABASE IF NOT EXISTS {config.SNOWSQL_DATABASE};
CREATE SCHEMA IF NOT EXISTS {config.SNOWSQL_SCHEMA};
USE WAREHOUSE {config.SNOWSQL_WAREHOUSE};
USE DATABASE {config.SNOWSQL_DATABASE};
USE SCHEMA {config.SNOWSQL_SCHEMA};
CREATE OR REPLACE TABLE {config.TABLE_NAME} (
    loan_id STRING,
    customer_id STRING,
    loan_status STRING,
    current_loan_amount FLOAT,
    term STRING,
    credit_score FLOAT,
    years_in_job STRING,
    home_ownership STRING,
    annual_income FLOAT,
    purpose STRING,
    MONTHLY_DEBT STRING,
	YEARS_CREDIT_HISTORY INTEGER,
	MONTHS_SINCE_LAST_DELINQUENT INTEGER,
	NUMBER_OPEN_ACCOUNTS INTEGER,
	NUMBER__CREDIT_PROBLEMS INTEGER,
	CURRENT_CREDIT_BALANCE INTEGER,
	MAXIMUM_OPEN_CREDIT INTEGER,
	BANKRUPTCIES INTEGER,
	TAX_LIENS INTEGER
);
"""

def create_table():
    """Creates the loan_data table in Snowflake with the defined schema."""
    print(f"[INFO] Creating table `{config.TABLE_NAME}` in Snowflake...")

    try:
        result = subprocess.run(
            [
                "snowsql",
                "-a", config.SNOWSQL_ACCOUNT,
                "-u", config.SNOWSQL_USER,
                "-q", CREATE_TABLE_SQL
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print("[INFO] Table creation successful.")
    except subprocess.CalledProcessError as e:
        print("[ERROR] Failed to create table:\n", e.stderr)
        raise


def upload_and_copy_to_snowflake():
    abs_path = os.path.abspath(config.CSV_PATH)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"CSV file not found at: {abs_path}")

    sql_commands = f"""
        USE WAREHOUSE {config.SNOWSQL_WAREHOUSE};
        USE DATABASE {config.SNOWSQL_DATABASE};
        USE SCHEMA {config.SNOWSQL_SCHEMA};
        PUT file://{abs_path} @%{config.TABLE_NAME} AUTO_COMPRESS=TRUE;
        COPY INTO {config.TABLE_NAME}
        FROM @%{config.TABLE_NAME}
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF = ('n/a', '', 'NA', '#VALUE!'))
        ON_ERROR = 'CONTINUE';
    """

    try:
        result = subprocess.run(
            [
                "snowsql",
                "-a", config.SNOWSQL_ACCOUNT,
                "-u", config.SNOWSQL_USER,
                "-q", sql_commands
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print("[INFO] Snowflake load successful:\n", result.stdout)

    except subprocess.CalledProcessError as e:
        print("[ERROR] Snowflake load failed:\n", e.stderr)
        raise

def verify_load():
    check_sql = f"""
    USE DATABASE {config.SNOWSQL_DATABASE};
    USE SCHEMA {config.SNOWSQL_SCHEMA};

    SELECT COUNT(*) AS row_count FROM {config.TABLE_NAME};
    """

    result = subprocess.run(
        [
            "snowsql",
            "-a", config.SNOWSQL_ACCOUNT,
            "-u", config.SNOWSQL_USER,
            "-q", check_sql
        ],
        capture_output=True,
        text=True
    )

    print("[INFO] Verification result:")
    print(result.stdout)

def load_pipeline():
    """Main load pipeline: create table, then upload and copy data."""
    create_table()
    upload_and_copy_to_snowflake()
    verify_load()


if __name__ == "__main__":
    load_pipeline()
