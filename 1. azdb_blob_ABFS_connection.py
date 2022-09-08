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

display(df.select("Call Number","Incident Number","Entry DtTm"))

# COMMAND ----------

#Apply Schema on dataframe
from pyspark.sql.types import *
dfschema = StructType([
    StructField("Call Number",IntegerType(),True),
    StructField("Incident Number",IntegerType(),True),
    StructField("Entry DtTm",DateType() ,True)
]) 

df = spark.read.format("csv")\
             .option("header","true")\
             .schema(dfschema).load(f"abfss://{blobstorage_container_name}@{blobstorage_account_name}.dfs.core.windows.net/Fire_Department_Calls_for_Service.csv")\
.select("Call Number","Incident Number","Entry DtTm")


display(df)
df.printSchema()


# COMMAND ----------

blobstorage_account_name = "azstorageblob1"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","blobsastoken")
blobstorage_container_name = "azdbcontainerblob"

spark.conf.set(f"fs.azure.account.auth.type.{blobstorage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{blobstorage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{blobstorage_account_name}.dfs.core.windows.net", blobstorage_account_accesskey)


dbutils.fs.ls("abfss://azdbcontainerblob@azstorageblob1.dfs.core.windows.net")



# COMMAND ----------

    dbutils.secrets.list("azdb_secretscope")

# COMMAND ----------


