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

def read_and_insert_to_duckdb(table, file_path, conn):
    """Read a CSV file and insert its data into DuckDB."""
    try:
        print(f"Importing file {file_path} into {table}")
        df = pd.read_csv(file_path, sep=';', encoding='ISO-8859-1')
        # Insert data into the 'bronze' table
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        conn.execute(f"INSERT INTO {table} SELECT * FROM df")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def drop_duplicates_from_table(table, conn):
    """Remove duplicate rows from the specified table in DuckDB."""
    try:
        conn.execute(f"""
            CREATE TABLE {table}_temp AS
            SELECT DISTINCT * FROM {table};
            DROP TABLE {table};
            ALTER TABLE {table}_temp RENAME TO {table};
        """)
        print(f"Removed duplicates from {table}")
    except Exception as e:
        print(f"Error removing duplicates from {table}: {e}")

def main():
    # Define the folder paths and output filename
    data_source_folder = "/workspaces/valuation/data/staging"
    data_destination_folder = "/workspaces/valuation/data"
    output_filename = "dfp.duckdb"
    db_path = os.path.join(data_destination_folder, output_filename)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Define the file patterns to search for BP (Balance Sheet)
    file_patterns = [
        "*_cia_aberta_BPA_con_*.csv",
        "*_cia_aberta_BPP_con_*.csv",
        "*_cia_aberta_DRE_con_*.csv",
        "*_cia_aberta_DFC_MD_con_*.csv",
        "*_cia_aberta_DFC_MI_con_*.csv"
    ]

    # Get all CSV file paths
    file_paths = get_csv_file_paths(data_source_folder, file_patterns)

    # Insert data from each file into the DuckDB database
    for file_path in file_paths:

        if "_BPA_" in file_path:
            table_name = "bronze_dfp_bp"
        elif "_BPP_" in file_path:
            table_name = "bronze_dfp_bp"
        elif "_DRE_" in file_path:
            table_name = "bronze_dfp_dre"
        elif "_DFC_MD_" in file_path:
            table_name = "bronze_dfp_dfc_md"
        elif "_DFC_MI_" in file_path:
            table_name = "bronze_dfp_dfc_mi"
        
        read_and_insert_to_duckdb(table_name, file_path, conn)

    # Drop duplicates from tables
    tables_list = ["bronze_dfp_bp", "bronze_dfp_dre", "bronze_dfp_dfc_md", "bronze_dfp_dfc_mi"]
    for table in tables_list:
        # Drop duplicates from the table
        drop_duplicates_from_table(table, conn)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
