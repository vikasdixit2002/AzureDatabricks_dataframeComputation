# Databricks notebook source
# MAGIC %run "./Configs/config"

# COMMAND ----------

# MAGIC %run "./Utilities/general-helper"

# COMMAND ----------

execute_notebook(bronze_notebook, 60, {}, bronze_layer)

# COMMAND ----------

print("test")

# COMMAND ----------


