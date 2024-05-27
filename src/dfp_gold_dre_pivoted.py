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

    # Create or connect to the DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Clear the destination table dropping it
    drop_tables_from_duckdb([destination_table], conn)

    # Read data
    df = conn.sql(f"SELECT * FROM {source_table}")

    # Filter data
    df_current_values = df.filter("""
    ORDEM_EXERC = 'ÚLTIMO' and
    PERIODO_MESES = 3
    """)

    df_pivot = conn.sql(f"""
        FROM(
            PIVOT df_current_values
            ON DS_CONTA IN ('Receita de Venda de Bens e/ou Serviços')
            USING sum(VL_CONTA)
                        GROUP BY CNPJ_CIA, DENOM_CIA, GRUPO_DFP, MOEDA, ESCALA_MOEDA, DT_REFER, DT_INI_EXERC, DT_FIM_EXERC, VERSAO, CD_CVM
        )

    """)

    df_pivot = conn.sql('SELECT *, "Receita de Venda de Bens e/ou Serviços" AS RECEITA FROM df_pivot')

    columns = [item for item in df_pivot.columns if item != 'Receita de Venda de Bens e/ou Serviços']

    df_next_revenue = conn.sql(f"""
        SELECT {columns},
            lead(RECEITA) AS RECEITA_FUTURA
                OVER (
                    PARTITION BY CD_CVM
                    ORDER BY )
    """)

    # Close the DuckDB connection
    conn.close()

if __name__ == "__main__":
    main()
