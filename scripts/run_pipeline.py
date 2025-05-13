import pandas as pd
# from bs4 import BeautifulSoup
from src.extract import download_dataset
from src.load import upload_and_copy_to_snowflake

def main():
    dataset_csv_path = download_dataset()
    upload_and_copy_to_snowflake()


if __name__ == "__main__":
    main()
