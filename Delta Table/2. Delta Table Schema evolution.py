# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists golddb;
# MAGIC 
# MAGIC create table golddb.sales;

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
# MAGIC select count(1) from golddb.sales 

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table extended golddb.sales 

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into golddb.sales 
# MAGIC select brand,'','','',array(1, 2, 3),'','','','','' from parquet.`dbfs:/databricks-datasets/amazon/test4K/*.*`

# COMMAND ----------

.write.format("Delta").saveAsTabledf = spark.read.format("csv").option("inferschema",True).option("header",True).load("dbfs:/databricks-datasets/amazon/users/part-r-00000-f8d9888b-ba9e-47bb-9501-a877f2574b3c.csv").write.format("Delta").saveAsTable
display(df)

# COMMAND ----------

dbfs:/databricks-datasets/amazon/users/part-r-00000-f8d9888b-ba9e-47bb-9501-a877f2574b3c.csv%sql 
select * from csv.`dbfs:/databricks-datasets/amazon/users/part-r-00000-f8d9888b-ba9e-47bb-9501-a877f2574b3c.csv` header= True

# COMMAND ----------


