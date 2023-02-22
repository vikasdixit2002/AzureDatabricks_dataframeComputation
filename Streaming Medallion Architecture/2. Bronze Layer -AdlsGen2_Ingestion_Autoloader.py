# Databricks notebook source
# MAGIC %run "./Configs/config"

# COMMAND ----------

# MAGIC %run "./Utilities/general-helper"

# COMMAND ----------

connect_bronzelayer(bronzestorage_account_name,bronzestorage_account_accesskey)

# COMMAND ----------

#Blob Storage using access key
blobstorage_account_name = "azstoragegen2"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","gen2accesskey")
blobstorage_container_name = "source"

spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

df=spark.read.format("csv")\
             .option("header","true")\
             .option("InferSchema","true")\
.load(f"abfss://{blobstorage_container_name}@{blobstorage_account_name}.dfs.core.windows.net/Source_AutoInsuranceFeed/2023/02/13/Auto_Insurance_Claims.csv")

display(df)

# COMMAND ----------

dbutils.secrets.list("azdb_secretscope")

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------



# COMMAND ----------


