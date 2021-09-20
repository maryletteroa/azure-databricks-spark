# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying JSON files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), False),
                                        StructField("constructorId", IntegerType(), False),
                                        StructField("number", IntegerType(), False),
                                        StructField("position", IntegerType(), False),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True),
])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlmr/raw/qualifying/

# COMMAND ----------

# set multiline optio to true since

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dlmr/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyId, raceId, driverId, constructorId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlmr/processed/qualifying")
