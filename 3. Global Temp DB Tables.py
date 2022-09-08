# Databricks notebook source
#Blob Storage using access key
blobstorage_account_name = "azstorageblob1"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","blobaccesskey")
blobstorage_container_name = "azdbcontainerblob"

spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

df=spark.read.format("csv")\
             .option("header","true")\
             .option("InferSchema","true")\
             .load(f"abfss://{blobstorage_container_name}@{blobstorage_account_name}.dfs.core.windows.net/Fire_Department_Calls_for_Service.csv")

df.createOrReplaceGlobalTempView("FireDeptInformation")



# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC Select * from global_temp.FireDeptInformation
