# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# define schema using DDL format derived from Hive
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema)\
.json(f"{raw_folder_path}/constructors.json")

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

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df \
      .withColumnRenamed("constructorId", "constructor_id") \
      .withColumnRenamed("constructorRef", "constructor_ref") \
      .withColumn("data_source", lit(v_data_source))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")
