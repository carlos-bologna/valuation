import os, sys
import duckdb

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Add the parent directory to the system path
sys.path.append(parent_dir)

from common.transformations import drop_tables_from_duckdb, drop_duplicates_from_table

DATA_SOURCE_FOLDER = "/workspaces/valuation/data/staging/stocks"
DATA_DESTINATION_FOLDER = "/workspaces/valuation/data"

DATA_SOURCE_FILENAME = "historical_prices.csv"
DATA_DESTINATION_FILENAME = "stock_prices.duckdb"
DATA_DESTINATION_TABLENAME = "bronze_stock_prices"

def get_csv_file_paths(source_folder):
    folder_names = [name for name in os.listdir(source_folder) if os.path.isdir(os.path.join(source_folder, name))]
    return folder_names

def read_and_insert_to_duckdb(table, file_path, conn):
    """Read a CSV file and insert its data into DuckDB."""
    try:
        print(f"Importing file {file_path} into {table}")
        # Insert data into the 'bronze' table
       
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_csv_auto('{file_path}') LIMIT 0")
        conn.execute(f"INSERT INTO {table} SELECT * FROM read_csv_auto('{file_path}')")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")


def main():
    # Define the folder paths and output filename
    db_path = os.path.join(DATA_DESTINATION_FOLDER, DATA_DESTINATION_FILENAME)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Drop all tables the clear up the database
    tables_list = [DATA_DESTINATION_TABLENAME]
    drop_tables_from_duckdb(tables_list, conn)

    # Get all CSV file paths
    stock_list = get_csv_file_paths(DATA_SOURCE_FOLDER)

    # Insert data from each file into the DuckDB database
    for stock in stock_list:

        file_path = os.path.join(DATA_SOURCE_FOLDER, stock, DATA_SOURCE_FILENAME)

        read_and_insert_to_duckdb(DATA_DESTINATION_TABLENAME, file_path, conn)

    # Drop duplicates from tables
    for table in tables_list:
        # Drop duplicates from the table
        drop_duplicates_from_table(table, conn)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
