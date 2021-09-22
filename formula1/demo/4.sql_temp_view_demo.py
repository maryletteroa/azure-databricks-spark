# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using SQL
# MAGIC 
# MAGIC #### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# race_results_df.createTempView("v_race_results") # error when notebook is run again because view already exists

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# spark.sql returs a dataframe

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# can use a python variable using spark.sql
# which is not allowed in %sql
# this makes the command dynamic

# this is a temporary view
# only valid during the session

# use if the scope is just this notebook

p_race_year = 2019
race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temporary View
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

# Global temp view is available within the entire application
# in the context of databricks, an application is all notebooks attached to the cluster
# i.e. access even if cluster has been restarted (new session)

# use if other notebooks is using this view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# spark registers global views under global_temp db

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results

# COMMAND ----------

spark.sql("SELECT * \
FROM global_temp.gv_race_results ").show()
