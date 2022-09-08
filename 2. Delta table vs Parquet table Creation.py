# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC Create database if not exists Fire_dep_DB

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC Create table if not exists Fire_dep_DB.FireRecords
# MAGIC (
# MAGIC call_number INT,
# MAGIC Incident_Number INT,
# MAGIC Entry_DtTm DATE
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO Fire_dep_DB.FireRecords VALUES(1,1,"2018-08-09");

# COMMAND ----------

#Blob Storage using access key
blobstorage_account_name = "azstorageblob1"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","blobaccesskey")
blobstorage_container_name = "azdbcontainerblob"

spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Fire_dep_DB.CallerIDDetail
# MAGIC   USING PARQUET
# MAGIC LOCATION 'abfss://azdbcontainerblob@azstorageblob1.dfs.core.windows.net/userdata1.parquet'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Fire_dep_DB.CallerIDDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Fire_dep_DB.CallerInfo
# MAGIC (id int)
# MAGIC   USING PARQUET;
# MAGIC   
# MAGIC   insert into Fire_dep_DB.CallerInfo values (1);
