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

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()
