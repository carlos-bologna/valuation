import os, sys
import duckdb

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Add the parent directory to the system path
sys.path.append(parent_dir)

from common.transformations import drop_tables_from_duckdb

DATA_SOURCE_FOLDER = "/workspaces/valuation/data"
DATA_DESTINATION_FOLDER = "/workspaces/valuation/data"

DATA_SOURCE_FILENAME = "stock_prices.duckdb"
DATA_DESTINATION_FILENAME = "stock_prices.duckdb"

DATA_SOURCE_TABLENAME = "silver_stock_prices"
DATA_DESTINATION_TABLENAME = "gold_stock_price_labeled"

def read_table(tablename, conn):
    
    df = conn.sql(f"SELECT * FROM {tablename}")

    return df

def copy_table(source_table, destination_table, conn):
    try:
       
        conn.execute(f"CREATE TABLE IF NOT EXISTS {destination_table} AS SELECT * FROM {source_table}")

        print(f"Copying content of the table {source_table} to {destination_table}")
        
    except Exception as e:
        print(f"Error copying table {source_table}: {e}")


def main():
    
    db_path = os.path.join(DATA_SOURCE_FOLDER, DATA_SOURCE_FILENAME)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Drop all tables the clear up the database
    tables_list = [DATA_DESTINATION_TABLENAME]
    drop_tables_from_duckdb(tables_list, conn)

    # Read Data
    df = read_table(DATA_SOURCE_TABLENAME, conn)

    # Create a new column 'y' with values of the next record ordered by the 'Date' column
    query = """
    SELECT 
        *,
        LEAD(Open, 1) OVER (PARTITION BY Ticker ORDER BY Date) AS y
    FROM 
        df
    """

    # Execute the query and fetch the results into a new table
    conn.execute(f"CREATE TABLE {DATA_DESTINATION_TABLENAME} AS " + query)

    # Rename the column 'Date' to 'ds'
    conn.execute(f"ALTER TABLE {DATA_DESTINATION_TABLENAME} RENAME COLUMN Date TO ds")

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
