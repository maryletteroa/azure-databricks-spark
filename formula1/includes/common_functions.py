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

# COMMAND ----------

#check based on primary record
# always speficy the merge column so queries will be quicker (merge is an expensive operation)
# additional race_id or partitioncolumn could help spark find the right partition
# dynamic when using variable, static when e.g. using tgt.race_id = 1053

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartionPruning","true")
  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
      input_df.alias("src"),
      merge_condition) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
