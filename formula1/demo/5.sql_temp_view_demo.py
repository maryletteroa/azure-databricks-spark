# Databricks notebook source
# demonstrate that global temp view is accessible in another notebook 
# using the same cluster

# also available when cluster is attached or re-attached

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;
