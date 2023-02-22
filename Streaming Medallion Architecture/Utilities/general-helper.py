# Databricks notebook source
def execute_notebook(notebook, timeout, mapvariables, layer):
    returnval = dbutils.notebook.run(notebook, timeout, mapvariables)
    if returnval == "Success":
        print(f"{layer} notebook executed successfully")
    else:
        print(f"{layer} notebook failed")
        dbutils.notebook.exit(returnval)

# COMMAND ----------

#Blob Storage using access key
def connect_bronzelayer(bronzestorage_account_name,bronzestorage_account_accesskey):
    spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

# COMMAND ----------

#Blob Storage using access key
def load_data(bronzestorage_account_name,bronzestorage_account_accesskey):
    spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

# COMMAND ----------


df=spark.read.format("csv")\
             .option("header","true")\
             .option("InferSchema","true")\
.load(f"abfss://{blobstorage_container_name}@{blobstorage_account_name}.dfs.core.windows.net/Source_AutoInsuranceFeed/2023/02/13/Auto_Insurance_Claims.csv")

display(df)
