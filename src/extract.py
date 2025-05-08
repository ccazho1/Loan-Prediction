import os
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

def download_dataset():
    out_dir = 'data/raw/'
    # Download from Kaggle
    api = KaggleApi()
    api.authenticate()

    dataset = 'tristanali/loan-predicition-info'

    # donwload without unzipping, so we can inspect and extract dataset name
    api.dataset_download_files(dataset, path=out_dir)

    zip_path = os.path.join(out_dir, dataset.split("/")[-1] + ".zip")

    # Open and inspect zip contents
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Get the name of the first file inside
        file_inside_zip = z.namelist()[0]
        print(f"Found file in zip: {file_inside_zip}")

        # Extract file_inside_zip and stores in out_dir
        z.extract(file_inside_zip, path=out_dir)

        # creates path to modify
        extracted_path = os.path.join(out_dir, file_inside_zip)

        # rename path
        rename_to = file_inside_zip.lower().replace(" ", "_")
        renamed_path = os.path.join(out_dir, rename_to)
        os.rename(extracted_path, renamed_path)
        print(f"Renamed to: {rename_to}")

    # remove zip file from out_dir
    os.remove(zip_path)
