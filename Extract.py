from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import SparkSession, DataFrame # type: ignore
import os  # For checking file existence

file_path = r'raw_data\ASPIRE_DATA_csv.csv'

def create_spark_session(app_name: str = "ASPIRE_ETL") -> SparkSession:
    """
    Create and return a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def extract_csv_to_df(spark: SparkSession, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame: # type: ignore
    """
    Extract data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): An active Spark session.
        file_path (str): Path to the CSV file.
        header (bool): Whether the CSV file contains a header row.
        infer_schema (bool): Whether to infer the schema automatically.

    Returns:
        DataFrame: A Spark DataFrame containing the CSV data, or None if an error occurs.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at path: {file_path}")
        return None
    try:
        df = spark.read.csv(file_path, header=header, inferSchema=infer_schema)
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

if __name__ == "__main__":
    spark = create_spark_session()
    data_df = extract_csv_to_df(spark, file_path)

    if data_df:
        data_df.printSchema()
        data_df.show(5)

    spark.stop()