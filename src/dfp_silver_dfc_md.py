import os
import duckdb
from common.transformations import drop_tables_from_duckdb, remove_suffix

def main():
    # Define the folder paths and output filename
    data_source_folder = "/workspaces/valuation/data"
    output_filename = "dfp.duckdb"
    db_path = os.path.join(data_source_folder, output_filename)
    source_table = "bronze_dfp_dfc_md"
    destination_table = "silver_dfp_dfc_md"

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Clear the destination table dropping it
    drop_tables_from_duckdb([destination_table], conn)

    # Copy data from bronze table to silver table with type conversions and value transformations
    copy_query = f"""
    CREATE TABLE {destination_table} AS
    SELECT
        *,
        CAST(DT_REFER AS DATE) AS DT_REFER,
        CAST(DT_INI_EXERC AS DATE) AS DT_INI_EXERC,
        CAST(DT_FIM_EXERC AS DATE) AS DT_FIM_EXERC,
        CAST(VERSAO AS INTEGER) AS VERSAO,
        CAST(CD_CVM AS INTEGER) AS CD_CVM,
        CASE
            WHEN ST_CONTA_FIXA = 'S' THEN TRUE
            WHEN ST_CONTA_FIXA = 'N' THEN FALSE
            ELSE NULL
        END AS ST_CONTA_FIXA
    FROM {source_table};
    """
    conn.execute(copy_query)

    # Get the list of columns in the destination table
    columns_query = f"""
    PRAGMA table_info({destination_table});
    """
    columns_info = conn.execute(columns_query).fetchall()
    columns = [col[1] for col in columns_info if col[1].endswith("_1")]

    #Remove suffix "_1" from the columns.
    remove_suffix(columns, destination_table, conn)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
