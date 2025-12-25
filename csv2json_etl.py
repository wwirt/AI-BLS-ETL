'''
This script performs an ETL (Extract, Transform, Load) operation to convert CSV files to JSON format.
It reads CSV files from a specified input directory, transforms the data into JSON format with the correct data types.
It then saves the resulting JSON files to a specified output directory.
'''

'''
This script performs an ETL (Extract, Transform, Load) operation to convert CSV files to JSON format.
It reads CSV files from a specified input directory, transforms the data into JSON format with the correct data types.
It then saves the resulting JSON files to a specified output directory.
'''
import polars as pl
import os

def etl_bls_data():
    # Extract - Define input and output file paths
    input_dir = 'input'
    output_dir = 'output'
    input_file = os.path.join(input_dir, 'USBLSStats.csv')
    output_file = os.path.join(output_dir, 'OccupationStats.json')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define data types for columns
    str_cols = [
        'AREA', 'AREA_TITLE', 'PRIM_STATE', 'OCC_CODE', 'OCC_TITLE', 'O_GROUP'
    ]
    int_cols = [
        'TOT_EMP', 'A_MEAN', 'A_PCT10', 'A_PCT25', 'A_MEDIAN', 'A_PCT75', 'A_PCT90'
    ]
    float_cols = [
        'EMP_PRSE', 'JOBS_1000', 'LOC_QUOTIENT', 'H_MEAN', 'MEAN_PRSE', 'H_PCT10', 'H_PCT25', 'H_MEDIAN', 'H_PCT75', 'H_PCT90'
    ]

    dtypes = {
        **{col: pl.Utf8 for col in str_cols},
        **{col: pl.Int64 for col in int_cols},
        **{col: pl.Float64 for col in float_cols}
    }

    try:
        # --- Extract ---
        occ_stats = pl.scan_csv(input_file, null_values=['#', '*', '**'], dtypes=dtypes)

        # --- Transform ---
        # Filter by OCC_CODE
        filtered_occ_stats = occ_stats.filter(pl.col("OCC_CODE") == "00-0000")

        # Trigger the computation.
        transformed_occ_stats = filtered_occ_stats.collect()

        # --- Load ---
        transformed_occ_stats.write_json(output_file)

        print(f"Successfully processed data and saved to {output_file}")
        print(f"Number of records processed: {len(transformed_occ_stats)}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    etl_bls_data()