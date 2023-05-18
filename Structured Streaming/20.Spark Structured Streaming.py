# Databricks notebook source
spark.conf.set("spark.sql.streaming.schemaInference", "true")

# COMMAND ----------

streamingdf = spark.readStream\
                  .format("csv")\
                  .option("header",True)\
                  .option("Inferschema",True)\
                  .load("/mnt/azdbcontainergen2_point/*/*.csv")

# COMMAND ----------

deltastreamingtableingestion_query = streamingdf.writeStream\
                               .format("delta")\
                               .outputMode("append")\
                               .option("checkpointLocation", "/mnt/azdbcontainergen2_point/chkpoint")\
                               . toTable("deltastreamingtable")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from deltastreamingtable

# COMMAND ----------

deltastreamingtableingestion_query.stop()
