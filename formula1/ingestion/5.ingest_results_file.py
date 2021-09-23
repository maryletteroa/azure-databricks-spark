# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read json file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), False)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform data

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .drop(col("statusId"))\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Write to parquet file

# COMMAND ----------

# collects takes all the data and puts these in the driver node memory
# use for small data not millions of rows

for race_id_list in results_final_df.select("race_id").distinct().collect():
  # check if table exists before dropping
  # takes a while specialy for the checkout data since this loops through all race_ids
  if (spark._jsparkSession.catalog().tableExists("f1_processed.results")): 
    spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("overwrite")\
# .partitionBy("race_id") \
# .parquet(f"{processed_folder_path}/results")
results_final_df.write.mode("append")\
.partitionBy("race_id") \
.format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
