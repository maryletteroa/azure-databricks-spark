# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Ingest circuts.csv file

# COMMAND ----------

# show mounth points
display(dbutils.fs.mounts())

# COMMAND ----------

# show contents of raw folder
%fs
ls /mnt/formula1dlmr/raw

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# read circuits.csv as a dataframe using DataFrame API
# .option() enables additional arguments
# using two options result in two spark jobs resulting in performance limitations

circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("dbfs:/mnt/formula1dlmr/raw/circuits.csv")

# COMMAND ----------

# for production level projects better to specify the schema manually
# specially if you're loading large datasets

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# StructType specifies rows
# StructField species columns

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", IntegerType(), True),
  StructField("url", StringType(), True),
])

# COMMAND ----------

# specify the schema

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("dbfs:/mnt/formula1dlmr/raw/circuits.csv")

# COMMAND ----------

# determine data type of objects
type(circuits_df)

# COMMAND ----------

# list top 20 records (default)
circuits_df.show()

# COMMAND ----------

# print object as a formatted table
display(circuits_df)

# COMMAND ----------

# print schema
circuits_df.printSchema()

# COMMAND ----------

# show summary statistics
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only the required columns

# COMMAND ----------

# by specifying columns as string

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# by using dot notation

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

# by passing as list

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

# by using col

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# first method only allows selecting the column
# other methods allows further methods
# e.g. changing the column name using .alias()

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns as required

# COMMAND ----------

# aside from .alias()
circuits_renamed_df  = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
  .withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("race_country", "country") \
  .withColumnRenamed("lat", "latitude") \
  .withColumnRenamed("lng", "longitude") \
  .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_df)
