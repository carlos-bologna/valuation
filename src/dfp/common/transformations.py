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

def drop_tables_from_duckdb(tables, conn):
    """Drop a list of tables from a DuckDB connection."""
    try:
        for table in tables:
            print(f"Dropping table {table}")
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        print("All specified tables have been dropped successfully.")
    except Exception as e:
        print(f"Error dropping tables: {e}")

def remove_suffix(columns, table, conn):
    for column in columns:
        new_column = column.rstrip('_1')
        # Drop the new column if it already exists
        drop_column_query = f"""
        ALTER TABLE {table} DROP COLUMN IF EXISTS {new_column};
        """
        conn.execute(drop_column_query)
        # Rename the column
        rename_query = f"""
        ALTER TABLE {table} RENAME COLUMN {column} TO {new_column};
        """
        conn.execute(rename_query)

