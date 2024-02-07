# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_timestamp
db_name="dim_azuread"
table_name="azure_ad_test"
mountPoint="/mnt/hr-hcm-nonprod"
Container_Gold="hcm-nonprod-gold"
current_datetime = datetime.now()
current_date = current_datetime.strftime("%Y-%m-%d")

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {db_name}.{table_name}""")
spark.sql(f"""CREATE EXTERNAL TABLE {db_name}.{table_name}
          USING csv
          OPTIONS('header' 'true',
          'inferSchema' 'true')
          LOCATION '/mnt/hr-hcm-nonprod/hcmdata-nonprod/AzureAD/Azure_AD_Export_Test.csv'
          """)

# COMMAND ----------

# MAGIC %sql select * from dim_azuread.azure_ad_test;

# COMMAND ----------

df1 = spark.read.table("db_hcm.kroger_azureimo_data")
df2= spark.read.table("dim_azuread.azure_ad_test")
display(df1.count())
display(df2.count())
display(df1)
display(df2)

# COMMAND ----------

# Assuming 'key_column' is the common column between df1 and df2
# 'column_to_append' is the column you want to append from df2 to df1

# Performing a join operation
result_df = df1.join(df2.select("Person_Number", "EUID"), df1["Person_Number"] == df2["Person_Number"], "inner")

# Selecting the columns you want in the final DataFrame
result_df = result_df.select(df1["*"], df2["EUID"].alias("EUID"))

# If you want to overwrite df1 with the result
display(result_df.count())
display(result_df.distinct().count())
display(result_df.distinct())
