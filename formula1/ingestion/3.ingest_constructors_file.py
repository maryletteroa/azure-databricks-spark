# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# define schema using DDL format derived from Hive
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema)\
.json("/mnt/formula1dlmr/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# by dropping the column instead of selecting relevant columns
constructor_dropped_df = constructor_df.drop("url")

# alternative ways
# useful if there are multiple dataframes with the same column name
# constructor_dropped_df = constructor_df.drop(constructor_df["url"])

# constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlmr/processed/constructors")
