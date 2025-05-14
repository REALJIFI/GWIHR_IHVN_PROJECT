from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import row_number # type: ignore
from pyspark.sql.window import Window # type: ignore

def create_facility_dimension(df: DataFrame) -> DataFrame:
    """Creates the Facility dimension table."""
    dim_facility = df.select('Location_ID', 'State', 'LGA', 'Datim_Code', 'Facility_Name').distinct()
    return dim_facility

def create_patient_dimension(df: DataFrame) -> DataFrame:
    """Creates the Patient dimension table."""
    patient_dim = df.select(
        'Patient_ID', 'Patient_code', 'Patient_Hospital_No', 'ANCNoIdentifier', 'ANCNoConceptID',
        'HTS_No', 'Sex', 'Date_Of_Birth', 'Registration_Phone_No', 'Next_Of_Kin_Phone_No',
        'Treatment_Supporter_Phone_No', 'Biometric_Captured', 'Biometric_Capture_Date', 'Valid_Capture'
    ).distinct()
    return patient_dim

def create_viral_load_dimension(df: DataFrame) -> DataFrame:
    """Creates the Viral Load dimension table."""
    viral_load_dim = df.select(
        'ViralLoad_ID', 'Current_Viral_Load', 'Viral_Load_Encounter_Date', 'Viral_Load_Sample_Collection_Date',
        'Viral_Load_Reported_Date', 'Result_Date', 'Assay_Date', 'Approval_Date', 'Viral_Load_Indication'
    ).distinct()
    return viral_load_dim

def create_regimen_dimension(df: DataFrame) -> DataFrame:
    """Creates the Regimen dimension table."""
    regimen_dim = df.select(
        'Regimen_ID', 'Initial_Regimen_Line', 'Initial_Regimen', 'Current_Regimen_Line', 'Current_Regimen',
        'Initial_FirstLine_Regimen', 'Initial_FirstLine_Regimen_Date', 'Initial_SecondLine_Regimen',
        'Initial_SecondLine_Regimen_Date'
    ).distinct()
    return regimen_dim

def create_tb_status_dimension(df: DataFrame) -> DataFrame:
    """Creates the TB Status dimension table."""
    tb_status_dim = df.select(
        'TBStatus_ID', 'TB_Status', 'TB_Status_Date', 'Baseline_INH_StartDate', 'Baseline_INH_StopDate',
        'Current_INH_StartDate', 'Current_INH_Outcome', 'Current_INH_Outcome_Date',
        'Last_INH_Dispensed_Date', 'Baseline_TB_Treatment_StartDate', 'Baseline_TB_Treatment_StopDate'
    ).distinct()
    return tb_status_dim

def create_patient_care_fact(df: DataFrame, dim_facility: DataFrame, patient_dim: DataFrame,
                             viral_load_dim: DataFrame, regimen_dim: DataFrame,
                             tb_status_dim: DataFrame) -> DataFrame:
    """Creates the Patient Care fact table."""
    window_spec = Window.orderBy("Patient_ID")

    fact_patient_care = df.join(dim_facility, on="Location_ID", how="left") \
        .join(patient_dim, on="Patient_ID", how="left") \
        .join(viral_load_dim, on="ViralLoad_ID", how="left") \
        .join(regimen_dim, on="Regimen_ID", how="left") \
        .join(tb_status_dim, on="TBStatus_ID", how="left") \
        .select(
            "Patient_ID", "Location_ID", "Regimen_ID", "ViralLoad_ID", "TBStatus_ID",
            "Age_At_Start_Of_ART_Years",
            "Age_At_Start_Of_ART_Months",
            "Care_Entry_Point",
            "KP_Type",
            "Months_On_ART",
            "Date_Transferred_In",
            "Transfer_In_Status",
            "ART_Start_Date",
            "Last_Pickup_Date",
            "Last_Visit_Date",
            "Days_Of_ARV_Refill",
            "Pill_Balance",
            "InitialCD4Count",
            "InitialCD4CountDate",
            "CurrentCD4Count",
            "CurrentCD4CountDate",
            "Last_EAC_Date",
            "Pregnancy_Status",
            "Pregnancy_Status_Date",
            "EDD",
            "Last_Delivery_Date",
            "LMP",
            "Gestation_Age_Weeks",
            "Patient_Outcome",
            "Patient_Outcome_Date",
            "Current_ART_Status",
            "Dispensing_Modality",
            "Facility_Dispensing_Modality",
            "DDD_Dispensing_Modality",
            "MMD_Type",
            "Date_Returned_To_Care",
            "Date_Of_Termination",
            "Pharmacy_Next_Appointment",
            "Clinical_Next_Appointment",
            "Current_Age_Years",
            "Current_Age_Months",
            "Mark_As_Deceased",
            "Mark_As_Deceased_Death_Date",
            "Current_Weight",
            "Current_Weight_Date",
            "Last_Viral_Load_Sample_CollectionForm_Date",
            "Last_Sample_Taken_Date",
            "OTZ_Enrollment_Date",
            "OTZ_Outcome_Date",
            "Enrollment_Date",
            "Last_Pickup_Date_Previous_Quarter",
            "Drug_Duration_Previous_Quarter",
            "Patient_Outcome_Previous_Quarter",
            "Patient_Outcome_Date_Previous_Quarter",
            "ART_Status_Previous_Quarter",
            "Quantity_Of_ARV_Dispensed_LastVisit",
            "Frequency_Of_ARV_Dispensed_LastVisit",
            "current_art_status_with_pillbalance",
            "Recapture_Date",
            "Recapture_Count"
        ).withColumn("Fact_ID", row_number().over(window_spec))

    return fact_patient_care

def create_data_frame(df_clean: DataFrame):
    """Creates the dimension tables and the fact table."""
    dim_facility = create_facility_dimension(df_clean)
    patient_dim_table = create_patient_dimension(df_clean)
    viral_load_dim_table = create_viral_load_dimension(df_clean)
    regimen_dim_table = create_regimen_dimension(df_clean)
    tb_status_dim_table = create_tb_status_dimension(df_clean)
    fact_patient_care = create_patient_care_fact(
        df_clean, dim_facility, patient_dim_table, viral_load_dim_table,
        regimen_dim_table, tb_status_dim_table
    )
    return dim_facility, patient_dim_table, viral_load_dim_table, regimen_dim_table, tb_status_dim_table, fact_patient_care
