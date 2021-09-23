# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") \
.select("race_id", "circuit_id", "race_year", "race_name", "race_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") \
.select("circuit_id", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality") \
.select("driver_id", "driver_name", "driver_number", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") \
.select("constructor_id", "team")

# COMMAND ----------

# results is incremental load so need to process the new date
# add filter(...)

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.select("result_id", "result_race_id", "driver_id", "constructor_id", "grid", "fastest_lap", "race_time", "points", "position")

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id)

# COMMAND ----------

final_df = results_df.join(circuits_races_df, results_df.result_race_id == circuits_races_df.race_id) \
        .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = add_ingestion_date(final_df) \
.withColumnRenamed("ingestion_date", "created_date") \
.drop("constructor_id", "driver_id", "circuit_id", "result_id")

# COMMAND ----------

# re-arrange columns according to requirements
final_df = final_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "created_date")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
overwrite_partition(final_df, "f1_presentation.race_results", "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
