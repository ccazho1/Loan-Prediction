import pandas as pd
# from bs4 import BeautifulSoup
from src.extract import download_dataset
from src.load import load_pipeline

def main():
    download_dataset()
    load_pipeline()


if __name__ == "__main__":
    main()
