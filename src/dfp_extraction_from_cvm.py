"""
Download and Extract Financial Data Files

This script automates the download and extraction of financial data files from the Comissão de Valores Mobiliários (CVM) database.
The CVM, Brazil's securities and exchange commission, offers public access to various financial documents submitted by publicly traded companies.
These documents are crucial for financial analysis, research, and regulatory compliance.
"""

import os
import requests
import zipfile
import shutil
from datetime import datetime

# Directory paths for storing zip files and extracted data
ZIP_FOLDER = "/tmp/zip"
DATA_FOLDER = "/workspaces/valuation/data/staging"
START_YEAR = 2016
CURRENT_YEAR = datetime.now().year
INTERVAL_TYPES = ["itr", "dfp"]

def setup_directories(zip_folder, data_folder):
    """
    Create necessary directories for storing zip files and extracted data.
    Remove existing directories if they already exist.
    """
    if os.path.exists(zip_folder):
        shutil.rmtree(zip_folder)
    if os.path.exists(data_folder):
        shutil.rmtree(data_folder)
    os.makedirs(zip_folder)
    os.makedirs(data_folder)

def download_and_extract_file(url, zip_folder, data_folder, local_filename):
    """
    Download a ZIP file from the given URL and extract its contents to the specified data folder.
    Delete the ZIP file after extraction.
    """
    zip_file_path = os.path.join(zip_folder, local_filename)

    # Check if the file already exists
    if not os.path.exists(zip_file_path):
        # Download the file
        with requests.get(url, stream=True) as r:
            if r.status_code == 404:
                print(f"Error 404: File not found at {url}. Skipping download.")
                return
            r.raise_for_status()
            with open(zip_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {zip_file_path}")

        # Extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(data_folder)
        print(f"Extracted to {data_folder}")

        # Delete the zip file
        os.remove(zip_file_path)
        print(f"Deleted {zip_file_path}\n")
    else:
        print(f"File {zip_file_path} already exists. Skipping download and extraction.")

def main():
    """
    Main function to handle the download and extraction of financial data files.
    """
    setup_directories(ZIP_FOLDER, DATA_FOLDER)

    for year in range(START_YEAR, CURRENT_YEAR + 1):
        for interval_type in INTERVAL_TYPES:
            formatted_url = f"http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{interval_type}/DADOS/{interval_type}_cia_aberta_{year}.zip"
            local_filename = f"{interval_type}_cia_aberta_{year}.zip"
            download_and_extract_file(formatted_url, ZIP_FOLDER, DATA_FOLDER, local_filename)

    # Cleanup: delete the zip folder after processing
    shutil.rmtree(ZIP_FOLDER)

if __name__ == "__main__":
    main()
