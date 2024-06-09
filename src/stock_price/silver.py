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

DATA_SOURCE_TABLENAME = "bronze_stock_prices"
DATA_DESTINATION_TABLENAME = "silver_stock_prices"

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

    # Do the transformation
    # So far, just copy from bronze
    copy_table(DATA_SOURCE_TABLENAME, DATA_DESTINATION_TABLENAME, conn)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
