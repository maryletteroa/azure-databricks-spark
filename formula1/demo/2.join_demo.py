# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# rename the column before joining to avoid redundant names
# between the two databases

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Joins

# COMMAND ----------

# Left Outer Join

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# Right Outer Join

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# Full Outer Join

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins

# COMMAND ----------

# semi joins only allows access of columns at the left side of the join
# hence no access to races_df.race_name, and races_df.round, must be removed
# similar to inner joins and selecting only columns from left dataframe

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Joins

# COMMAND ----------

# anything on the left dataframe not found in the right dataframe

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Joins

# COMMAND ----------

# gives  the cartesian product of the two dataframes
# int(races_df.count()) * int(circuits_df.count())

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

#

# COMMAND ----------

display(race_circuits_df)
# race_circuits_df.count()
