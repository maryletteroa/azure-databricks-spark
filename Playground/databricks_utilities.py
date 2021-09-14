# Databricks notebook source
# MAGIC %md
# MAGIC # dbutils commands

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# similar to `%fs ls`
dbutils.fs.ls("/")

# COMMAND ----------

# combine python with dbutils
for folder_name in dbutils.fs.ls("/"):
  print(folder_name)

# COMMAND ----------

# see all dbutils commands available
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("mount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils notebook

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# call the child notebook
# can be used to chain notebooks together
dbutils.notebook.run("./child_notebook", 10, {"input": "Called from main notebook"})

# COMMAND ----------


