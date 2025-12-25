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
    """
    Extracts, transforms, and loads BLS data from CSV to JSON.
    """
    # Define input and output paths
    input_dir = 'input'
    output_dir = 'output'
    input_file = os.path.join(input_dir, 'USBLSStats.csv')
    output_file = os.path.join(output_dir, 'TechOccupations.json')

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define columns to be cast to numeric types
    int_cols = [
        'TOT_EMP', 'A_MEAN', 'A_PCT10', 'A_PCT25', 'A_MEDIAN', 'A_PCT75', 'A_PCT90'
    ]
    float_cols = [
        'EMP_PRSE', 'JOBS_1000', 'LOC_QUOTIENT', 'H_MEAN', 'MEAN_PRSE',
        'H_PCT10', 'H_PCT25', 'H_MEDIAN', 'H_PCT75', 'H_PCT90'
    ]

    # Define dtypes for scanning to avoid extra casting pass
    dtypes = {
        **{col: pl.Utf8 for col in ['AREA', 'AREA_TITLE', 'PRIM_STATE', 'OCC_CODE', 'OCC_TITLE', 'O_GROUP']},
        **{col: pl.Int64 for col in int_cols},
        **{col: pl.Float64 for col in float_cols}
    }

    try:
        # --- Extract & Transform (Lazy) ---
        # The filter is applied during the read, minimizing data loaded into memory.
        lazy_df = pl.scan_csv(input_file, null_values=['#', '*'], dtypes=dtypes)

        # 1. Filter the DataFrame for Computer and Mathematical Occupations using OCC_CODE
        filtered_lazy_df = lazy_df.filter(pl.col("OCC_CODE") == "00-0000")

        # The casting is now handled by `dtypes` in scan_csv, so a separate
        # .with_columns for casting is no longer needed for performance.
        # We only need to trigger the computation.
        transformed_df = filtered_lazy_df.collect()

        # --- Load ---
        # Write the transformed DataFrame to a JSON file
        transformed_df.write_json(output_file)

        print(f"Successfully processed data and saved to {output_file}")
        print(f"Number of records processed: {len(transformed_df)}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    etl_bls_data()