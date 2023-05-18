# Databricks notebook source
import pyspark.sql.functions as sf

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists golddb;
# MAGIC
# MAGIC create table if not exists golddb.sales;

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/amazon/test4K/')

# COMMAND ----------

spark.read.format("parquet")\
               .option("inferschema",True)\
               .load("dbfs:/databricks-datasets/amazon/test4K/*.*")\
               .write\
               .format("Delta")\
               .option("mergeSchema", "true")\
               .mode("append")\
               .saveAsTable("golddb.sales")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from golddb.sales limit 10

# COMMAND ----------

# MAGIC %python 
# MAGIC tablename = 'golddb.sales'
# MAGIC spark.conf.set('tbl.tablename',tablename)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC set name= ${tbl.tablename}

# COMMAND ----------

# MAGIC %md Changing data type

# COMMAND ----------


df = spark.read.format("parquet")\
               .option("inferschema",True)\
               .load("dbfs:/databricks-datasets/amazon/test4K/*.*").withColumn("rating",sf.col("rating").cast("int"))
df.write.format("delta").option("overwriteschema",True).mode("overwrite").saveAsTable("golddb.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from golddb.sales

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select asin
# MAGIC ,brand
# MAGIC ,helpful
# MAGIC ,img
# MAGIC ,price
# MAGIC ,rating
# MAGIC ,review
# MAGIC ,time
# MAGIC ,title
# MAGIC user
# MAGIC   from parquet.`/databricks-datasets/amazon/test4K/*.*`

# COMMAND ----------


