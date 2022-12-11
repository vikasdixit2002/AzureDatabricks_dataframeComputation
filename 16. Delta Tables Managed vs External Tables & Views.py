# Databricks notebook source
import pyspark.sql.functions as sf
import pyspark.sql.types as st

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


# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS FlightInformation_DB

# COMMAND ----------

dfflightInfo = df.select([sf.col(dfcolumn).alias(dfcolumn.replace(" ","_") ) for dfcolumn in df.columns])

# COMMAND ----------


dfflightInfo.write.format("delta").saveAsTable("FlightInformation_DB.Flight_Info")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY FlightInformation_DB.Flight_Info

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE  FlightInformation_DB.Flight_Info

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE EXTENDED FlightInformation_DB.Flight_Info;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL FlightInformation_DB.Flight_Info;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from FlightInformation_DB.Flight_Info@v0

# COMMAND ----------

# MAGIC %sql 
# MAGIC update FlightInformation_DB.Flight_Info
# MAGIC set Received_DtTm = CURRENT_TIMESTAMP()

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DESCRIBE  HISTORY FlightInformation_DB.Flight_Info

# COMMAND ----------

dfupdatedflightinfo = spark.sql("select * from  FlightInformation_DB.Flight_Info@v0")
display(dfupdatedflightinfo)

# COMMAND ----------

dfupdatedflightinfo = spark.read.format("delta").option("versionasof","0").load("dbfs:/user/hive/warehouse/flightinformation_db.db/flight_info")
display(dfupdatedflightinfo)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dfflightInfo.write.format("delta")\
# MAGIC    .option("path","abfss://azdbcontainerblob@azstorageblob1.dfs.core.windows.net/FlightInfoextrenaltable")\
# MAGIC    .saveAsTable("FlightInformation_DB.External_FileInfo")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table FlightInformation_DB.External_FileInfo_usingSQL 
# MAGIC location "abfss://azdbcontainerblob@azstorageblob1.dfs.core.windows.net/FlightInfoextrenaltable"

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended FlightInformation_DB.External_FileInfo_usingSQL 

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table FlightInformation_DB.External_FileInfo_usingjson
# MAGIC location "/dbfs/FileStore/array_of_struct.json"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended FlightInformation_DB.External_FileInfo_usingjson

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table FlightInformation_DB.External_FileInfo_usingjsonwithoutschema
# MAGIC select * from json.`/FileStore/array_of_struct.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED FlightInformation_DB.External_FileInfo_usingjsonwithoutschema

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DESCRIBE TABLE EXTENDED FlightInformation_DB.External_FileInfo

# COMMAND ----------

dfview = spark.read.format("Delta")\
              .load("abfss://azdbcontainerblob@azstorageblob1.dfs.core.windows.net/FlightInfoextrenaltable")

display(dfview)

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE VIEW FlightInformation_DB.Persistent_FlightInfo
# MAGIC AS 
# MAGIC SELECT * FROM FlightInformation_DB.External_FileInfo 

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables from FlightInformation_DB

# COMMAND ----------


