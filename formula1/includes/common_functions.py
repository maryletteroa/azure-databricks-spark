# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def rearrange_partition_column(input_df, partition_column):
  idx = input_df.schema.names.index(partition_column)
  names = input_df.schema.names[:idx] + input_df.schema.names[idx+1:] + [partition_column]
  return input_df.select(names)

# COMMAND ----------

def overwrite_partition(input_df, table_name:str, partition_column):
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  output_df = rearrange_partition_column(input_df, partition_column)
  if (spark._jsparkSession.catalog().tableExists(table_name)):
    output_df.write.mode("overwrite").insertInto(table_name)
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(table_name)

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
  .distinct() \
  .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list
