import os
import psycopg2 # type: ignore
from dotenv import load_dotenv # type: ignore
from pyspark.sql import DataFrame, SparkSession # type: ignore

# Load environment variables
load_dotenv()

def get_db_connection():
    """Creates and returns a PostgreSQL database connection."""
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Database connection established.")
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def close_db_connection(conn):
    """Closes the PostgreSQL database connection."""
    if conn:
        conn.close()
        print("Database connection closed.")

def create_hiv_response_schema(conn):
    """Creates the HIV_response schema if it doesn't exist."""
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS HIV_response;")
        conn.commit()
        print("HIV_response schema created or already exists.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating schema: {e}")
    finally:
        cursor.close()

def drop_existing_tables(conn):
    """Drops existing tables in the HIV_response schema in reverse dependency order."""
    cursor = conn.cursor()
    drop_queries = [
        "DROP TABLE IF EXISTS HIV_response.Fact_Patient_Care;",
        "DROP TABLE IF EXISTS HIV_response.TBStatus_Dim_Table;",
        "DROP TABLE IF EXISTS HIV_response.ViralLoad_Dim_Table;",
        "DROP TABLE IF EXISTS HIV_response.Regimen_Dim_Table;",
        "DROP TABLE IF EXISTS HIV_response.Patient_Dim_Table;",
        "DROP TABLE IF EXISTS HIV_response.DimFacility;"
    ]
    try:
        for query in drop_queries:
            cursor.execute(query)
        conn.commit()
        print("Existing tables dropped.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error dropping tables: {e}")
    finally:
        cursor.close()

def create_dimension_tables(conn):
    """Creates the dimension tables in the HIV_response schema."""
    cursor = conn.cursor()
    create_queries = [
        """
        CREATE TABLE IF NOT EXISTS HIV_response.DimFacility (
            Location_ID BIGINT PRIMARY KEY,
            State VARCHAR(100),
            LGA VARCHAR(100),
            Datim_Code VARCHAR(50),
            Facility_Name VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS HIV_response.Patient_Dim_Table (
            Patient_ID BIGINT PRIMARY KEY,
            Patient_Code VARCHAR(100),
            Patient_Hospital_No VARCHAR(100),
            ANCNoIdentifier VARCHAR(100),
            ANCNoConceptID VARCHAR(100),
            HTS_No VARCHAR(100),
            Sex VARCHAR(10),
            Date_Of_Birth DATE,
            Registration_Phone_No VARCHAR(100),
            Next_Of_Kin_Phone_No VARCHAR(100),
            Treatment_Supporter_Phone_No VARCHAR(100),
            Biometric_Captured BOOLEAN,
            Biometric_Capture_Date DATE,
            Valid_Capture BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS HIV_response.ViralLoad_Dim_Table (
            ViralLoad_ID BIGINT PRIMARY KEY,
            Current_Viral_Load VARCHAR(50),
            Viral_Load_Encounter_Date DATE,
            Viral_Load_Sample_Collection_Date DATE,
            Viral_Load_Reported_Date DATE,
            Result_Date DATE,
            Assay_Date DATE,
            Approval_Date DATE,
            Viral_Load_Indication VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS HIV_response.Regimen_Dim_Table (
            Regimen_ID BIGINT PRIMARY KEY,
            Initial_Regimen_Line VARCHAR(100),
            Initial_Regimen VARCHAR(100),
            Current_Regimen_Line VARCHAR(100),
            Current_Regimen VARCHAR(100),
            Initial_FirstLine_Regimen VARCHAR(100),
            Initial_FirstLine_Regimen_Date DATE,
            Initial_SecondLine_Regimen VARCHAR(100),
            Initial_SecondLine_Regimen_Date DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS HIV_response.TBStatus_Dim_Table (
            TBStatus_ID BIGINT PRIMARY KEY,
            TB_Status VARCHAR(100),
            TB_Status_Date DATE,
            Baseline_INH_StartDate DATE,
            Baseline_INH_StopDate DATE,
            Current_INH_StartDate DATE,
            Current_INH_Outcome VARCHAR(100),
            Current_INH_Outcome_Date DATE,
            Last_INH_Dispensed_Date DATE,
            Baseline_TB_Treatment_StartDate DATE,
            Baseline_TB_Treatment_StopDate DATE
        );
        """
    ]
    try:
        for query in create_queries:
            cursor.execute(query)
        conn.commit()
        print("Dimension tables created.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating dimension tables: {e}")
    finally:
        cursor.close()

def create_fact_table(conn):
    """Creates the Fact_Patient_Care table in the HIV_response schema."""
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS HIV_response.Fact_Patient_Care (
        Fact_ID SERIAL PRIMARY KEY,
        Location_ID BIGINT REFERENCES HIV_response.DimFacility(Location_ID),
        Patient_ID BIGINT REFERENCES HIV_response.Patient_Dim_Table(Patient_ID),
        ViralLoad_ID BIGINT REFERENCES HIV_response.ViralLoad_Dim_Table(ViralLoad_ID),
        Regimen_ID BIGINT REFERENCES HIV_response.Regimen_Dim_Table(Regimen_ID),
        TBStatus_ID BIGINT REFERENCES HIV_response.TBStatus_Dim_Table(TBStatus_ID),
        Age_At_Start_Of_ART_Years INT,
        Age_At_Start_Of_ART_Months INT,
        Care_Entry_Point VARCHAR(100),
        KP_Type VARCHAR(100),
        Months_On_ART INT,
        Date_Transferred_In DATE,
        Transfer_In_Status VARCHAR(100),
        ART_Start_Date DATE,
        Last_Pickup_Date DATE,
        Last_Visit_Date DATE,
        Days_Of_ARV_Refill INT,
        Pill_Balance INT,
        InitialCD4Count INT,
        InitialCD4CountDate DATE,
        CurrentCD4Count INT,
        CurrentCD4CountDate DATE,
        Last_EAC_Date DATE,
        Pregnancy_Status VARCHAR(100),
        Pregnancy_Status_Date DATE,
        EDD DATE,
        Last_Delivery_Date DATE,
        LMP DATE,
        Gestation_Age_Weeks INT,
        Patient_Outcome VARCHAR(100),
        Patient_Outcome_Date DATE,
        Current_ART_Status VARCHAR(100),
        Dispensing_Modality VARCHAR(100),
        Facility_Dispensing_Modality VARCHAR(100),
        DDD_Dispensing_Modality VARCHAR(100),
        MMD_Type VARCHAR(100),
        Date_Returned_To_Care DATE,
        Date_Of_Termination DATE,
        Pharmacy_Next_Appointment DATE,
        Clinical_Next_Appointment DATE,
        Current_Age_Years INT,
        Current_Age_Months INT,
        Mark_As_Deceased BOOLEAN,
        Mark_As_Deceased_Death_Date DATE,
        Current_Weight DECIMAL(10, 2),
        Current_Weight_Date DATE,
        Last_Viral_Load_Sample_CollectionForm_Date DATE,
        Last_Sample_Taken_Date DATE,
        OTZ_Enrollment_Date DATE,
        OTZ_Outcome_Date DATE,
        Enrollment_Date DATE,
        Last_Pickup_Date_Previous_Quarter DATE,
        Drug_Duration_Previous_Quarter INT,
        Patient_Outcome_Previous_Quarter VARCHAR(100),
        Patient_Outcome_Date_Previous_Quarter DATE,
        ART_Status_Previous_Quarter VARCHAR(100),
        Quantity_Of_ARV_Dispensed_LastVisit INT,
        Frequency_Of_ARV_Dispensed_LastVisit VARCHAR(100),
        current_art_status_with_pillbalance VARCHAR(100),
        Recapture_Date DATE,
        Recapture_Count INT
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print("Fact table created.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating fact table: {e}")
    finally:
        cursor.close()

def create_database_tables():
    """Creates the schema and all necessary tables in the PostgreSQL database."""
    conn = get_db_connection()
    if conn:
        create_hiv_response_schema(conn)
        drop_existing_tables(conn)
        create_dimension_tables(conn)
        create_fact_table(conn)
        close_db_connection(conn)

def get_spark_session():
    """Creates and returns a SparkSession."""
    url = "jdbc:postgresql://localhost:5432/ASPIRE_PROJECT"
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    spark = SparkSession.builder \
        .appName("HIV Data Load") \
        .config("spark.jars", r"postgresql-42.7.4.jar") \
        .getOrCreate()
    return spark, url, properties

def load_data_to_table(spark: SparkSession, df: DataFrame, url: str, table_name: str, properties: dict):
    """Writes the Spark DataFrame to the specified PostgreSQL table."""
    try:
        df.write.jdbc(url=url, table=f"HIV_response.{table_name}", mode="append", properties=properties)
        print(f"Data loaded successfully to HIV_response.{table_name}")
    except Exception as e:
        print(f"Error loading data to {table_name}: {e}")

def load_data(dim_facility: DataFrame, patient_dim_table: DataFrame, viral_load_dim_table: DataFrame,
              regimen_dim_table: DataFrame, tb_status_dim_table: DataFrame, fact_patient_care: DataFrame):
    """Loads data from Spark DataFrames into PostgreSQL tables."""
    spark, url, properties = get_spark_session()

    load_data_to_table(spark, dim_facility, url, "DimFacility", properties)
    load_data_to_table(spark, patient_dim_table, url, "Patient_Dim_Table", properties)
    load_data_to_table(spark, viral_load_dim_table, url, "ViralLoad_Dim_Table", properties)
    load_data_to_table(spark, regimen_dim_table, url, "Regimen_Dim_Table", properties)
    load_data_to_table(spark, tb_status_dim_table, url, "TBStatus_Dim_Table", properties)
    load_data_to_table(spark, fact_patient_care, url, "Fact_Patient_Care", properties)

    spark.stop()
    print('All data loading processes completed.')
