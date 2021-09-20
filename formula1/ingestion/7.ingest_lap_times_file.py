# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

# set multiline optio to true since

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1dlmr/raw/lap_times") # specify folder containing csvs
        # can also specify path with regex "/mnt/formula1dlmr/raw/lap_times/lap_times_split*.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlmr/processed/lap_times")
