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

    # Define the OCC_CODE to filter for Computer and Mathematical Occupations
    tech_occ_code_filter = "15-0000"

    # Define columns to be cast to numeric types
    int_cols = [
        'TOT_EMP', 'A_MEAN', 'A_PCT10', 'A_PCT25', 'A_MEDIAN', 'A_PCT75', 'A_PCT90'
    ]
    float_cols = [
        'EMP_PRSE', 'JOBS_1000', 'LOC_QUOTIENT', 'H_MEAN', 'MEAN_PRSE',
        'H_PCT10', 'H_PCT25', 'H_MEDIAN', 'H_PCT75', 'H_PCT90'
    ]

    try:
        # --- Extract ---
        # Read the CSV file, treating '#' and '*' as null values.
        # Polars will infer column types.
        df = pl.read_csv(input_file, null_values=['#', '*'])

        # --- Transform ---
        # 1. Filter the DataFrame for the specified OCC_CODE
        filtered_df = df.filter(pl.col("OCC_CODE") == tech_occ_code_filter)

        # 2. Cast columns to their correct data types
        # The `strict=False` argument will insert nulls on conversion errors.
        transformed_df = filtered_df.with_columns([
            pl.col('AREA').cast(pl.Utf8)
        ] + [
            pl.col(c).cast(pl.Int64, strict=False) for c in int_cols
        ] + [
            pl.col(c).cast(pl.Float64, strict=False) for c in float_cols
        ])

        # --- Load ---
        # Write the transformed DataFrame to a JSON file
        # `orient='records'` creates a JSON array of objects, similar to results.json
        transformed_df.write_json(output_file, orient='records', pretty=True)

        print(f"Successfully processed data and saved to {output_file}")
        print(f"Number of records processed: {len(transformed_df)}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    etl_bls_data()