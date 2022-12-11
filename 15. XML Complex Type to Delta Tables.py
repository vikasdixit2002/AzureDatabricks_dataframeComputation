# Databricks notebook source
path = "/FileStore/File_Carrier/"
fileformat = "com.databricks.spark.xml"

import pyspark.sql.functions as sf
from pyspark.sql import Window

# COMMAND ----------



df_Client_Information = spark.read.format(fileformat)\
                  .option("rootTag","XDB")\
                  .option("rowTag","ROOT")\
                  .load(path)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS Client_UserInfoDB

# COMMAND ----------

df_client = df_Client_Information.select("Key.AGENT_CLIENT_USERID_C",
                                         sf.row_number().over(Window.orderBy("Key.AGENT_CLIENT_USERID_C")).alias("Client_id"))
df_client.write.format("Delta").saveAsTable("Client_UserInfoDB.Client")

# COMMAND ----------

df_demographic = df_Client_Information.select("Key.AGENT_CLIENT_USERID_C","DEMOGRAPHIC.*")
df_demographic.write.format("Delta").saveAsTable("Client_UserInfoDB.Client_Demographic")

# COMMAND ----------

df_addressArray = df_Client_Information.select("Key.AGENT_CLIENT_USERID_C",sf.explode("ADDRESS.ADD").alias("Adrs"))
df_address = df_addressArray.select("AGENT_CLIENT_USERID_C","Adrs.*")
df_address.write.format("Delta").saveAsTable("Client_UserInfoDB.Client_Address")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from Client;
# MAGIC select * from Client_Demographic;
# MAGIC select * from Client_UserInfoDB.Client_Address;
