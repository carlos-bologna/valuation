import glob
import os
import pandas as pd
import duckdb

def get_csv_file_paths(source_folder, patterns):
    """Retrieve all CSV file paths that match the given patterns."""
    file_paths = []
    for pattern in patterns:
        file_paths.extend(glob.glob(os.path.join(source_folder, pattern)))
    return file_paths

def read_and_insert_to_duckdb(file_path, conn):
    """Read a CSV file and insert its data into DuckDB."""
    try:
        df = pd.read_csv(file_path, sep=';', encoding='ISO-8859-1')
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def main():
    # Define the folder paths and output filename
    data_source_folder = "/workspaces/valuation/data/staging"
    data_destination_folder = "/workspaces/valuation/data"
    output_filename = "dfp.duckdb"
    db_path = os.path.join(data_destination_folder, output_filename)

    # Define the file patterns to search for
    file_patterns = [
        "*_cia_aberta_BPA_con_*.csv",
        "*_cia_aberta_BPP_con_*.csv",
        "*_cia_aberta_DRE_con_*.csv",
        "*_cia_aberta_DFC_con_*.csv"
    ]

    # Get all CSV file paths
    file_paths = get_csv_file_paths(data_source_folder, file_patterns)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Insert data from each file into the DuckDB database
    for file_path in file_paths:
        read_and_insert_to_duckdb(file_path, conn)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
