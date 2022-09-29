# Databricks notebook source
#Sample Example
df = spark.read.format("csv")\
               .option("header","true")\
               .load("/databricks-datasets/airlines/part-00000")
display(df.count())
dfsample = df.sample(0.1,False,0)
display(dfsample.count())

# COMMAND ----------

#Sample By
base_df = df.where(df.UniqueCarrier.isin("AA","DL","PS"))

df_sampleby = base_df.sampleBy(df.UniqueCarrier,{"AA":0.18,
                                  "DL":0.158},
                                 0)
display( df_sampleby.select(df_sampleby.UniqueCarrier)\
                 .groupBy(df_sampleby.UniqueCarrier)\
                 .count())

# COMMAND ----------

#Stratification 
(df1, df2, df3) = df.randomSplit([0.25,0.50,0.25], 0)

# COMMAND ----------

#Combining
df4 = df1.union(df2).union(df3)
display(df4.count())

# COMMAND ----------


