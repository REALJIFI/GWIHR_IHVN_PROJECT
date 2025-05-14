from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.functions import col, when, to_date, monotonically_increasing_id # type: ignore
from pyspark.sql.functions import col, when, to_date # type: ignore
from pyspark.sql.types import StringType, DoubleType # type: ignore


def convert_phone_numbers(df: DataFrame) -> DataFrame:
    """Converts phone number columns to string type."""
    phone_columns = ["RegistrationPhoneNo", "NextOfKinPhoneNo", "TreatmentSupporterPhoneNo"]
    for col_name in phone_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
    return df

def fill_and_convert_numeric_columns(df: DataFrame) -> DataFrame:
    """Fills null values in specified numeric columns with 0 and casts them to DoubleType."""
    numeric_columns = [
        "QuantityOfARVDispensedLastVisit", "RecaptureCount", "DrugDurationPreviousQuarter",
        "CurrentWeight(Kg)", "CurrentAgeMonths", "CurrentAgeYears", "CurrentViralLoad(c/ml)",
        "GestationAgeWeeks", "CurrentCD4Count", "InitialCD4Count", "PillBalance",
        "DaysOfARVRefil", "AgeAtStartOfARTYears", "HTSNo", "ANCNoConceptID", "ANCNoIdentifier"
    ]
    df = df.fillna(0, subset=[col for col in numeric_columns if col in df.columns])
    for col_name in [col for col in numeric_columns if col in df.columns]:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df

def fill_and_convert_text_columns(df: DataFrame) -> DataFrame:
    """Fills null values in specified text columns with 'unknown' and casts them to StringType."""
    text_columns = [
        "State", "LGA", "DatimCode", "FacilityName", "PatientUniqueID", "PatientHospitalNo",
        "Sex", "CareEntryPoint", "KPType", "TransferInStatus", "InitialRegimenLine", "InitialRegimen",
        "CurrentRegimenLine", "CurrentRegimen", "PregnancyStatus", "ViralLoadIndication",
        "PatientOutcome", "CurrentARTStatus", "DispensingModality", "FacilityDispensingModality",
        "DDDDispensingModality", "MMDType",
        "MarkAsDeseased", "TBStatus", "CurrentINHOutcome",
        "InitialFirstLineRegimen", "InitialSecondLineRegimen", "PatientOutcomePreviousQuarter",
        "ARTStatusPreviousQuarter", "FrequencyOfARVDispensedLastVisit", "CurrentARTStatusWithPillBalance"
    ]
    df = df.fillna("unknown", subset=[col for col in text_columns if col in df.columns])
    for col_name in [col for col in text_columns if col in df.columns]:
        df = df.withColumn(col_name, col(col_name).cast(StringType()))
    return df

def convert_binary_columns(df: DataFrame) -> DataFrame:
    """Converts binary string columns ('Yes', 'No') to boolean (True, False)."""
    binary_columns = ["BiometricCaptured", "ValidCapture", "MarkAsDeseased"]
    for col_name in binary_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(col(col_name) == "Yes", True)
                .when(col(col_name) == "No", False)
                .otherwise(None)
            )
    return df

def format_date_columns(df: DataFrame) -> DataFrame:
    """Converts specified date columns to DateType and fills nulls with '1899-12-30'."""
    date_columns = [
        "DateTransferredIn", "ARTStartDate", "LastPickupDate", "LastVisitDate", "InitialCD4CountDate",
        "CurrentCD4CountDate", "LastEACDate", "PregnancyStatusDate", "EDD", "LastDeliveryDate", "LMP",
        "ViralLoadEncounterDate", "ViralLoadSampleCollectionDate", "ViralLoadReportedDate", "ResultDate",
        "AssayDate", "ApprovalDate", "PatientOutcomeDate", "DateReturnedToCare", "DateOfTermination",
        "PharmacyNextAppointment", "ClinicalNextAppointment", "DateOfBirth", "MarkAsDeseasedDeathDate",
        "BiometricCaptureDate", "CurrentWeightDate", "TBStatusDate", "BaselineINHStartDate",
        "BaselineINHStopDate", "CurrentINHStartDate", "CurrentINHOutcomeDate", "LastINHDispensedDate",
        "BaselineTBTreatmentStartDate", "BaselineTBTreatmentStopDate", "LastViralLoadSampleCollectionFormDate",
        "LastSampleTakenDate", "OTZEnrollmentDate", "OTZOutcomeDate", "EnrollmentDate",
        "InitialFirstLineRegimenDate", "InitialSecondLineRegimenDate", "LastPickupDatePreviousQuarter",
        "PatientOutcomeDatePreviousQuarter", "RecaptureDate"
    ]
    for col_name in [col for col in date_columns if col in df.columns]:
        df = df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
        df = df.fillna({col_name: "1899-12-30"})
    return df

def rename_columns(df: DataFrame) -> DataFrame:
    """Renames specified columns for clarity."""
    rename_dict = {
        "PatientUniqueID": "Patient_code",
        "PatientHospitalNo": "Patient_Hospital_No",
        "FacilityName": "Facility_Name",
        "HTSNo": "HTS_No",
        "DateOfBirth": "Date_Of_Birth",
        "AgeAtStartOfARTYears": "Age_At_Start_Of_ART_Years",
        "AgeAtStartOfARTMonths": "Age_At_Start_Of_ART_Months",
        "CurrentAgeYears": "Current_Age_Years",
        "CurrentAgeMonths": "Current_Age_Months",
        "CareEntryPoint": "Care_Entry_Point",
        "KPType": "KP_Type",
        "BiometricCaptured": "Biometric_Captured",
        "BiometricCaptureDate": "Biometric_Capture_Date",
        "ValidCapture": "Valid_Capture",
        "MarkAsDeseased": "Mark_As_Deceased",
        "MarkAsDeseasedDeathDate": "Mark_As_Deceased_Death_Date",
        "RegistrationPhoneNo": "Registration_Phone_No",
        "NextofKinPhoneNo": "Next_Of_Kin_Phone_No",
        "TreatmentSupporterPhoneNo": "Treatment_Supporter_Phone_No",
        "DatimCode": "Datim_Code",
        "InitialRegimenLine": "Initial_Regimen_Line",
        "InitialRegimen": "Initial_Regimen",
        "InitialFirstLineRegimen": "Initial_FirstLine_Regimen",
        "InitialFirstLineRegimenDate": "Initial_FirstLine_Regimen_Date",
        "InitialSecondLineRegimen": "Initial_SecondLine_Regimen",
        "InitialSecondLineRegimenDate": "Initial_SecondLine_Regimen_Date",
        "CurrentRegimenLine": "Current_Regimen_Line",
        "CurrentRegimen": "Current_Regimen",
        "MonthsOnART": "Months_On_ART",
        "ARTStartDate": "ART_Start_Date",
        "CurrentARTStatus": "Current_ART_Status",
        "DispensingModality": "Dispensing_Modality",
        "FacilityDispensingModality": "Facility_Dispensing_Modality",
        "DDDDispensingModality": "DDD_Dispensing_Modality",
        "MMDType": "MMD_Type",
        "CurrentARTStatusWithPillBalance": "current_art_status_with_pillbalance",
        "CurrentViralLoad(c/ml)": "Current_Viral_Load",
        "ViralLoadEncounterDate": "Viral_Load_Encounter_Date",
        "ViralLoadSampleCollectionDate": "Viral_Load_Sample_Collection_Date",
        "ViralLoadReportedDate": "Viral_Load_Reported_Date",
        "ResultDate": "Result_Date",
        "AssayDate": "Assay_Date",
        "ApprovalDate": "Approval_Date",
        "ViralLoadIndication": "Viral_Load_Indication",
        "LastViralLoadSampleCollectionFormDate": "Last_Viral_Load_Sample_CollectionForm_Date",
        "LastSampleTakenDate": "Last_Sample_Taken_Date",
        "PregnancyStatus": "Pregnancy_Status",
        "PregnancyStatusDate": "Pregnancy_Status_Date",
        "EDD": "EDD",
        "LastDeliveryDate": "Last_Delivery_Date",
        "GestationAgeWeeks": "Gestation_Age_Weeks",
        "LastPickupDate": "Last_Pickup_Date",
        "LastVisitDate": "Last_Visit_Date",
        "DaysOfARVRefil": "Days_Of_ARV_Refill",
        "PillBalance": "Pill_Balance",
        "PharmacyNextAppointment": "Pharmacy_Next_Appointment",
        "ClinicalNextAppointment": "Clinical_Next_Appointment",
        "LastEACDate": "Last_EAC_Date",
        "OTZEnrollmentDate": "OTZ_Enrollment_Date",
        "OTZOutcomeDate": "OTZ_Outcome_Date",
        "EnrollmentDate": "Enrollment_Date",
        "DateTransferredIn": "Date_Transferred_In",
        "TransferInStatus": "Transfer_In_Status",
        "DateReturnedToCare": "Date_Returned_To_Care",
        "DateOfTermination": "Date_Of_Termination",
        "RecaptureDate": "Recapture_Date",
        "RecaptureCount": "Recapture_Count",
        "CurrentWeight(Kg)": "Current_Weight",
        "CurrentWeightDate": "Current_Weight_Date",
        "TBStatus": "TB_Status",
        "TBStatusDate": "TB_Status_Date",
        "PatientOutcome": "Patient_Outcome",
        "PatientOutcomeDate": "Patient_Outcome_Date",
        "BaselineINHStartDate": "Baseline_INH_StartDate",
        "BaselineINHStopDate": "Baseline_INH_StopDate",
        "CurrentINHStartDate": "Current_INH_StartDate",
        "CurrentINHOutcome": "Current_INH_Outcome",
        "CurrentINHOutcomeDate": "Current_INH_Outcome_Date",
        "LastINHDispensedDate": "Last_INH_Dispensed_Date",
        "BaselineTBTreatmentStartDate": "Baseline_TB_Treatment_StartDate",
        "BaselineTBTreatmentStopDate": "Baseline_TB_Treatment_StopDate",
        "LastViralLoadSampleCollectionFormDate": "Last_Viral_Load_Sample_CollectionForm_Date",
        "LastSampleTakenDate": "Last_Sample_Taken_Date",
        "InitialFirstLineRegimen": "Initial_FirstLine_Regimen",
        "InitialFirstLineRegimenDate": "Initial_FirstLine_Regimen_Date",
        "InitialSecondLineRegimen": "Initial_SecondLine_Regimen",
        "InitialSecondLineRegimenDate": "Initial_SecondLine_Regimen_Date",
        "LastPickupDatePreviousQuarter": "Last_Pickup_Date_Previous_Quarter",
        "DrugDurationPreviousQuarter": "Drug_Duration_Previous_Quarter",
        "PatientOutcomePreviousQuarter": "Patient_Outcome_Previous_Quarter",
        "PatientOutcomeDatePreviousQuarter": "Patient_Outcome_Date_Previous_Quarter",
        "ARTStatusPreviousQuarter": "ART_Status_Previous_Quarter",
        "QuantityOfARVDispensedLastVisit": "Quantity_Of_ARV_Dispensed_LastVisit",
        "FrequencyOfARVDispensedLastVisit": "Frequency_Of_ARV_Dispensed_LastVisit",
        "CurrentARTStatusWithPillBalance": "Current_ART_Status_With_PillBalance"
    }
    for old_name, new_name in rename_dict.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df

def generate_ids(df: DataFrame) -> DataFrame:
    """Generates unique IDs for Patient, Location, Regimen, ViralLoad, and TBStatus."""
    df = df.withColumn("Patient_ID", monotonically_increasing_id() + 1) \
           .withColumn("Location_ID", monotonically_increasing_id() + 1) \
           .withColumn("Regimen_ID", monotonically_increasing_id() + 1) \
           .withColumn("ViralLoad_ID", monotonically_increasing_id() + 1) \
           .withColumn("TBStatus_ID", monotonically_increasing_id() + 1)
    return df

def clean_dataframe(df: DataFrame) -> DataFrame:
    """Applies all the data cleaning and transformation steps."""
    df = convert_phone_numbers(df)
    df = fill_and_convert_numeric_columns(df)
    df = fill_and_convert_text_columns(df)
    df = convert_binary_columns(df)
    df = format_date_columns(df)
    df = rename_columns(df)
    df = generate_ids(df) # Add the ID generation step here
    return df

