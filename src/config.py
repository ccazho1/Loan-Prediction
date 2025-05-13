"""Store shared constants: filepaths, feature lists, model parameters, etc."""
"""Centralizes config for easier tuning and refactoring."""
import os
from dotenv import load_dotenv

load_dotenv()

SNOWSQL_ACCOUNT = os.getenv("SNOWSQL_ACCOUNT")
SNOWSQL_USER = os.getenv("SNOWSQL_USER")
CSV_PATH = os.getenv("CSV_PATH", "data/raw/loan_data.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "loan_data")
