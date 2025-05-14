# main_etl.py
import os
from pyspark.sql import SparkSession # type: ignore
from Extract import create_spark_session, extract_csv_to_df
from Transform import clean_dataframe
from create_df_table import create_data_frame
from load import create_database_tables, load_data

def main():
    """
    Main ETL script to extract, transform, and load HIV response data.
    """
    # --- Set Environment Variables ---
    os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk1.8.0_202'
    os.environ["PYSPARK_ALLOW_INSECURE_GATEWAY"] = "1"

    # --- Initialize Spark Session ---
    spark = SparkSession.builder \
        .appName("HIVResponseETL") \
        .config("spark.jars", r"postgresql-42.7.4.jar") \
        .getOrCreate()

    # --- EXTRACT ---
    raw_df = extract_csv_to_df(spark, r'raw_data\ASPIRE_DATA_csv.csv')

    if raw_df is None:
        print("Error during data extraction. ETL process aborted.")
        spark.stop()
        return

    # --- TRANSFORM ---
    df_cleaned = clean_dataframe(raw_df)

    # Create dimension and fact tables DataFrames
    dim_facility, patient_dim_table, viral_load_dim_table, \
    regimen_dim_table, tb_status_dim_table, fact_patient_care = create_data_frame(df_cleaned)

    # --- LOAD ---
    # Create database schema and tables
    create_database_tables()

    # Load data into the database tables
    load_data(dim_facility, patient_dim_table, viral_load_dim_table,
              regimen_dim_table, tb_status_dim_table, fact_patient_care)

    # Stop SparkSession
    spark.stop()
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()