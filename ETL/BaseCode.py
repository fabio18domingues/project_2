# Databricks notebook source
storage_account_name = "safutdata"
container_name = "futbasestorage"
mount_point = "/mnt/bronze"
account_key = "****"

configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net": account_key}

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/delta/bronze",
  mount_point = mount_point,
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;