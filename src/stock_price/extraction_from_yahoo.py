import os
import shutil
import yfinance as yf
from datetime import date
import sys

# Directory paths for storing zip files and extracted data
DATA_FOLDER = "/workspaces/valuation/data/staging/stocks"
START_DATE = "2016-01-01"
END_DATE = date.today()
DEFAULT_TICKER = "VALE5.SA"

def setup_directories(data_folder, ticker):
    # Create directory for the ticker, remove if it exists
    destination_path = os.path.join(data_folder, ticker)
    if os.path.exists(destination_path):
        shutil.rmtree(destination_path)
    os.makedirs(destination_path)

def extract_data(ticker, start_date, end_date, data_folder):
    # Download historical price data for the ticker
    df = yf.download(ticker, start=start_date, end=end_date)
    # Add the ticker as column
    df["Ticker"] = ticker
    # Save the data to a CSV file
    destination_path = os.path.join(data_folder, ticker, "historical_prices.csv")
    df.to_csv(destination_path)

def main():
    # Get the ticker from system arguments, default to DEFAULT_TICKER
    ticker = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TICKER
    
    setup_directories(DATA_FOLDER, ticker)
    extract_data(ticker, START_DATE, END_DATE, DATA_FOLDER)

if __name__ == "__main__":
    main()
