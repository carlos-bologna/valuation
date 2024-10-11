import os
import duckdb
import yaml
from flask import Flask, jsonify
import bronze
import silver
import gold_stock_price_labeled
import extraction_from_yahoo
import numpy as np
np.float_ = np.float64
from prophet import Prophet
import pandas as pd

app = Flask(__name__)

# Set Parameters
CONFIG_FILE = "/workspaces/valuation/config.yaml"
DATA_SOURCE_FOLDER = "/workspaces/valuation/data"
DATA_SOURCE_FILENAME = "stock_prices.duckdb"
DATA_SOURCE_TABLE = "gold_stock_price_labeled"

# Load config file
with open(CONFIG_FILE, 'r') as config_file:
    config = yaml.safe_load(config_file)

LOOK_FORWARD_DAYS = config['test_size_in_days'] # Number of days in the tail.

def get_training_data(ticker):

    # Read Data Source
    db_path = os.path.join(DATA_SOURCE_FOLDER, DATA_SOURCE_FILENAME)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=True)

    # Read data
    df = conn.sql(f"SELECT * FROM {DATA_SOURCE_TABLE} WHERE Ticker = '{ticker}'").fetchdf()

    # Close the DuckDB connection
    conn.close()

    # Remove Nulls
    df.dropna(inplace=True)

    # Sorting the dataframe by the date column 'ds'
    df = df.sort_values(by='ds')

    return df

def train_model(df):

    df.drop(columns=["Open", "Adj Close", "Ticker", "Volume", "High", "Low", "Close"], inplace=True)

    # Load the data into a Prophet model
    model = Prophet()
    model.fit(df)

    future_dates = pd.date_range(start=df['ds'].max() + pd.Timedelta(days=1),
                             periods=LOOK_FORWARD_DAYS,
                             freq='B')  # 'B' for business days, skipping weekends

    # Create a DataFrame with these future dates
    future = pd.DataFrame({'ds': future_dates})

    # Make predictions
    forecast = model.predict(future)

    # Get only the columns: ds, yhat, yhat_lower and yhat_upper
    forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    return forecast

@app.route('/predict/<ticker>', methods=['GET'])
def run_pipeline(ticker):
    try:
        # Extract historical data
        extraction_from_yahoo.main(ticker)

        # Run bronze layer processing
        bronze.main()

        # Run silver layer processing
        silver.main()

        # Run gold layer processing
        gold_stock_price_labeled.main()

        # Get training data
        df = get_training_data(ticker)

        forecast = train_model(df)

        # Convert forecast DataFrame to a dictionary
        forecast_dict = forecast.to_dict(orient='records')

        # Return the forecast data as JSON
        return jsonify({"forecast": forecast_dict}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)