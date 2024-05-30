import os
import duckdb
from common.transformations import drop_tables_from_duckdb

def main():
    # Define the folder paths and output filename
    data_source_folder = "/workspaces/valuation/data"
    output_filename = "dfp.duckdb"
    db_path = os.path.join(data_source_folder, output_filename)
    source_table = "silver_dfp_dre"
    destination_table = "gold_dfp_dre_pivoted"
    account_list = [
        ("Receita de Venda de Bens e/ou Serviços", "RECEITA"),
        ("Resultado Antes do Resultado Financeiro e dos Tributos", "EBIT"),
        ("Resultado Antes dos Tributos sobre o Lucro", "LAIR")
    ]
    # Extract the first position of each tuple and concatenate them in a variable
    account_str = ', '.join([f'"{item[0]}"' for item in account_list])

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Read data
    df = conn.sql(f"SELECT * FROM {source_table}")

    # Filter data
    df_current_values = df.filter("""
    ORDEM_EXERC = 'ÚLTIMO' and
    (PERIODO_MESES = 3 or PERIODO_MESES = 12)
    """)

    df_pivot = conn.sql(f"""
        FROM(
            PIVOT df_current_values
            ON DS_CONTA IN ({account_str})
            USING sum(VL_CONTA)
            GROUP BY CNPJ_CIA, DENOM_CIA, GRUPO_DFP, MOEDA, ESCALA_MOEDA, DT_REFER, DT_INI_EXERC, DT_FIM_EXERC, VERSAO, CD_CVM
        )

    """).fetchdf()

    # Create a dictionary from the list for easy lookup
    rename_dict = dict(account_list)

    # Rename the columns in the DuckDB dataframe
    df_pivot.rename(columns=rename_dict, inplace=True)

    # Clear the destination table dropping it, then write the new one.
    drop_tables_from_duckdb([destination_table], conn)

    # Create the new table in Duckdb from the dataframe.
    conn.execute(f"CREATE TABLE {destination_table} AS SELECT * FROM df_pivot")

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
