import os
import shutil
import investpy
from datetime import date
import sys

# Directory paths for storing zip files and extracted data
DATA_FOLDER = "/workspaces/valuation/data/staging/stocks"
START_DATE = "01/01/2016"
END_DATE = date.today().strftime("%d/%m/%Y")
DEFAULT_TICKER = "VALE5.SA"
COUNTRY = "brazil"

def setup_directories(data_folder, ticker):
    # Create directory for the ticker, remove if it exists
    destination_path = os.path.join(data_folder, ticker)
    if os.path.exists(destination_path):
        shutil.rmtree(destination_path)
    os.makedirs(destination_path)

def extract_data(ticker, start_date, end_date, data_folder, country=COUNTRY):
    # Download historical price data for the ticker
    df = investpy.stocks.get_stock_historical_data(
        stock=ticker,
        country=country,
        from_date=start_date,
        to_date=end_date
    )
    
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
