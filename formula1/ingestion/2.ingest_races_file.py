# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read races.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), False),
  StructField("name", StringType(), True),
  StructField("date", TimestampType(), True),
  StructField("time", TimestampType(), True),
  StructField("url", StringType(), True),
])

# COMMAND ----------

df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dlmr/raw/races.csv")

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform (select, rename, add columns)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat, current_timestamp

# COMMAND ----------

# df.withColumn("race_timestamp", concat(col("date"), lit(" "), col("time"))).show()
df  = df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")))) \
.withColumn("ingestion_date", current_timestamp())
df.show()

# COMMAND ----------

races_final_df = df.select(col("raceId").alias("race_id"),
  col("year").alias("race_year"),
  col("round"),
  col("circuitId").alias("circuit_id"),
  col("name"),
  col("race_timestamp"),
  col("ingestion_date")
)

races_df_final.show()

# COMMAND ----------

races_final_df.describe().show()

# COMMAND ----------

races_final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write csv file

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/formula1dlmr/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlmr/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlmr/processed/races"))
