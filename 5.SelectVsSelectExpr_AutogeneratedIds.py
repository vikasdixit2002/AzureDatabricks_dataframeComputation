# Databricks notebook source
from pyspark.sql.functions import expr

# COMMAND ----------

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


# COMMAND ----------

df_CFields = df.toDF(*[colname.replace(" ","") for colname in df.columns]) 
df_Prs = df_CFields.select(df_CFields.CallNumber,df_CFields.UnitID,expr("upper(CallType) as UCallType"))\
          .withColumn("ID",expr("row_number() over(order by CallNumber asc)"))
display(df_Prs)


# COMMAND ----------


