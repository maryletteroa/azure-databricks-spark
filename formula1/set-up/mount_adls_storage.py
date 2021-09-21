# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

# create a scope in the databricks workspace first
# by linking an Azure Key Vault resource
# hashes come from creating the Azure Service Principal

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")

# COMMAND ----------

storage_account_name = "formula1dlmr"
client_id = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
 "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
 "fs.azure.account.oauth2.client.id": f"{client_id}",
 "fs.azure.account.oauth2.client.secret": f"{client_secret}",
 "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlmr/raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlmr/processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# unmount
dbutils.fs.unmount("/mnt/formula1dlmr/raw")
dbutils.fs.unmount("/mnt/formula1dlmr/processed")
