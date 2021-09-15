# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest circuts.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# show mounth points
display(dbutils.fs.mounts())

# COMMAND ----------

# show contents of raw folder
%fs
ls /mnt/formula1dlmr/raw

# COMMAND ----------

# read circuits.csv as a dataframe using DataFrame API
# .option() enables additional arguments e.g. specifying that a header is present
circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/formula1dlmr/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

# list top 20 records (default)
circuits_df.show()

# COMMAND ----------

display(circuits_df)
