# Databricks notebook source
from datetime import datetime
mountPoint="/mnt/hr-hcm-nonprod"
Container="hcmdata-nonprod"
mountPoint_Gold="/mnt/hr-hcm-nonprod-gold"
Container_Gold="hcm-nonprod-gold"
db_name="db_hcm"
table_name="Kroger_AzureIMO_Data"

current_datetime = datetime.now()
formatted_date = current_datetime.strftime("%Y-%m-%d")

# COMMAND ----------

# Import functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = (rf"{mountPoint}/{Container}/")
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
db_name="db_hcm"
table_name = "Kroger_AzureIMO_Data"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)

#define schema explicitly

employee_schema = StructType([
    StructField("SSN", StringType(), True),
    StructField("Person_Number", IntegerType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Assignment_Status_Type", StringType(), True),
    StructField("Original_Hire_Date", DateType(), True),
    StructField("Current_Hire_Date", DateType(), True),
    StructField("Service_Award_Date", DateType(), True),
    StructField("Seniority_Date", DateType(), True),
    StructField("Location_Date", DateType(), True),
    StructField("Assignment_Category", StringType(), True),
    StructField("Employee_Category", StringType(), True),
    StructField("Job_Date", StringType(), True),
    StructField("Job_Code", StringType(), True),
    StructField("Job_Name", StringType(), True),
    StructField("Job_Level", StringType(), True),
    StructField("Management_Level", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Job_Family", StringType(), True),
    StructField("Union_Code", StringType(), True),
    StructField("Manager_Person_Number", StringType(), True),
    StructField("Manager_Name", StringType(), True),
    StructField("Work_Location_City", StringType(), True),
    StructField("Work_Location_State", StringType(), True),
    StructField("Division_Business_Unit", StringType(), True),
    StructField("District", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Legal_EmployerEIN", LongType(), True),
    StructField("Legal_Employer_Name", StringType(), True),
    StructField("Payroll_Name", StringType(), True),
    StructField("Grade_Name", StringType(), True),
    StructField("Grade_Ladder", StringType(), True),
    StructField("Grade_Step", IntegerType(), True),
    StructField("FLSA_Status", StringType(), True),
    StructField("Bonus_Target", StringType(), True),
    StructField("Pay_Rate", DecimalType(10, 2), True),
    StructField("Date_of_Birth", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Ethnicity", StringType(), True),
    StructField("Home_Address_Line_1", StringType(), True),
    StructField("Home_Address_Line_2", StringType(), True),
    StructField("Home_Address_City", StringType(), True),
    StructField("Home_Address_State", StringType(), True),
    StructField("Home_Address_Postal_Code", IntegerType(), True),
    StructField("Primary_Phone", StringType(), True),
    StructField("Personal_Email", StringType(), True),
    StructField("Work_Email", StringType(), True),
    StructField("Termination_Date", DateType(), True),
    StructField("Standard_Role", StringType(), True)
])

# Configure Auto Loader to ingest CSV data to a Delta table
df=(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header","true")
  .schema(employee_schema)
  .load(file_path)
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .toTable(table_name))

# COMMAND ----------

df = spark.read.table("default.kroger_azureimo_data")
display(df.count())
