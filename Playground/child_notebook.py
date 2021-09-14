# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("input", "", "Send the parameter")

# COMMAND ----------

input_param = dbutils.widgets.get("input")

# COMMAND ----------

print(input_param)

# COMMAND ----------

dbutils.notebook.exit(100)
