import os
import duckdb

def alter_column_type(con, table_name, column_name, new_type):

    # Create a temporary new column name
    temp_column_name = column_name + '_temp'

    try:
        # Step 1: Add a new column with the desired type
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {temp_column_name} {new_type};")

        # Step 2: Update the new column with converted data from the old column
        con.execute(f"UPDATE {table_name} SET {temp_column_name} = CAST({column_name} AS {new_type});")

        # Step 3: Drop the old column
        con.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")

        # Step 4: Rename the new column to the original column name
        con.execute(f"ALTER TABLE {table_name} RENAME COLUMN {temp_column_name} TO {column_name};")

        print(f"Column '{column_name}' type changed to '{new_type}' successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


def main():
    # Define the folder paths and output filename
    data_source_folder = "/workspaces/valuation/data"
    output_filename = "dfp.duckdb"
    db_path = os.path.join(data_source_folder, output_filename)

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Transform column types
    alter_column_type(conn, "bronze_dfp_dre", "DT_REFER", "DATE")
    alter_column_type(conn, "bronze_dfp_dre", "DT_INI_EXERC", "DATE")
    alter_column_type(conn, "bronze_dfp_dre", "DT_FIM_EXERC", "DATE")
    alter_column_type(conn, "bronze_dfp_dre", "VERSAO", "INTEGER")
    alter_column_type(conn, "bronze_dfp_dre", "CD_CVM", "INTEGER")

    # Convert S to true and N to false
    conn.execute("UPDATE bronze_dfp_dre SET ST_CONTA_FIXA = True WHERE ST_CONTA_FIXA = 'S'")
    conn.execute("UPDATE bronze_dfp_dre SET ST_CONTA_FIXA = False WHERE ST_CONTA_FIXA = 'N'")

    # Now, convert the ST_CONTA_FIXA column to BOOLEAN
    alter_column_type(conn, "bronze_dfp_dre", "ST_CONTA_FIXA", "BOOLEAN")

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
