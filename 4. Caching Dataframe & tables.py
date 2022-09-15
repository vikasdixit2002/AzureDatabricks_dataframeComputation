# Databricks notebook source
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

df.cache()

# COMMAND ----------

df.unpersist()

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from fire_dep_db.callerinfo

# COMMAND ----------

# MAGIC %sql
# MAGIC cache lazy table callerinfocache 
# MAGIC select * from fire_dep_db.callerinfo 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from callerinfocache

# COMMAND ----------


