# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read races.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), False),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform (select, rename, add columns)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat

# COMMAND ----------

races_df  = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time"))))

# COMMAND ----------

races_df = races_df.select(col("raceId").alias("race_id"),
  col("year").alias("race_year"),
  col("round"),
  col("circuitId").alias("circuit_id"),
  col("name"),
  col("race_timestamp"),
) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_final_df = add_ingestion_date(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

# partition output by race_year
# data are written into separate folders by year

races_final_df.write.mode("overwrite") \
  .partitionBy("race_year") \
  .parquet(f"{processed_folder_path}/races")

# COMMAND ----------

dbutils.notebook.exit("Success")
