# Databricks notebook source
import pyspark.sql.functions as sf 
from pyspark.sql import Window 

# COMMAND ----------

df = spark.read.format("csv")\
               .option("header","true")\
                .option("samplingRatio","0.1")\
                .load("/FileStore/tables/invoices.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC Simple and Windowing Functions

# COMMAND ----------

date = sf.to_date("InvoiceDate","dd-MM-yyyy H.mm")
NumInvoice = sf.countDistinct("InvoiceNo").alias("NumInvoice")
TotalQuantity = sf.sum("Quantity").alias("TotalQuantity")
InvoiceValue = sf.expr("round(sum(Quantity*UnitPrice),2)").alias("InvoiceValue")
InvoiceValue1 = sf.round(sf.sum(df.Quantity *df.UnitPrice),2).alias("InvoiceValue1")

df4 = df.where(sf.year(date)==2010)\
        .groupBy("country",sf.weekofyear(date).alias("week"))\
        .agg(NumInvoice,TotalQuantity,InvoiceValue,InvoiceValue1)
    
df4.write.format("parquet")\
          .mode("overwrite")\
          .save("/FileStore/aggregateddata/")


# COMMAND ----------

df_parquet = spark.read.format("parquet").\
                        load("/FileStore/aggregateddata/")

# COMMAND ----------

# MAGIC %md
# MAGIC Window Function

# COMMAND ----------


running_total_window = Window.partitionBy("country").\
                              orderBy("week").\
                              rowsBetween(Window.unboundedPreceding,Window.currentRow)

df_runningsum = df_parquet.withColumn("RunningSum",sf.sum("InvoiceValue").over(running_total_window))
display(df_runningsum)

# COMMAND ----------

# MAGIC %md
# MAGIC Top N records

# COMMAND ----------

ranking_window = Window.partitionBy("country")\
                        .orderBy("InvoiceValue")\
                        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

df_ranking = df4.withColumn("Rank",sf.dense_rank().over(ranking_window))\
                .where("Rank<=3")
display(df_ranking)

# COMMAND ----------


