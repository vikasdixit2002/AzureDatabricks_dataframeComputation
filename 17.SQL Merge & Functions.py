# Databricks notebook source
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql import Window

# COMMAND ----------

#Blob Storage using access key
blobstorage_account_name = "azstorageblob1"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","blobaccesskey")
blobstorage_container_name = "azdbcontainerblob"

spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

# COMMAND ----------

df=spark.read.format("csv")\
             .option("header","true")\
             .option("InferSchema","true")\
             .load(f"abfss://{blobstorage_container_name}@{blobstorage_account_name}.dfs.core.windows.net/Fire_Department_Calls_for_Service.csv")
dfformatted = df.select([sf.col(colname).alias(colname.replace(" ","_")) for colname in df.columns])

# COMMAND ----------

partitionWindow = Window.partitionBy().orderBy(dfformatted.Received_DtTm)
dfpartitioned = dfformatted.select("*",sf.row_number().over(partitionWindow).alias("ID"))

# COMMAND ----------

dfpartitioned.where("id<100").write.format("Delta").saveAsTable("FlightInformation_DB.Call_Details")

# COMMAND ----------

dfpartitioned.where("id>100 and id<200").write.format("Delta").saveAsTable("FlightInformation_DB.Call_Details_upd")

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from FlightInformation_DB.Call_Details_upd

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO FlightInformation_DB.Call_Details dest
# MAGIC  USING FlightInformation_DB.Call_Details_upd source
# MAGIC on  dest.id = source.id
# MAGIC WHEN matched then update set *
# MAGIC WHEN not matched then insert *

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC create function id_multiplier(mtr int)
# MAGIC returns int
# MAGIC 
# MAGIC return (mtr*0.5)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select id,id_multiplier(id) from FlightInformation_DB.Call_Details

# COMMAND ----------


