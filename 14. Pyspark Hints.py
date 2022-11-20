# Databricks notebook source
dataset1 = []
dataset2 = []

for i in range(10000):
    dataset1.append((i,"ds1"+ " " + str(i)))


for j in range(10000):
    dataset2.append((j,"ds2"+ " " + str(j)))


df1 = spark.createDataFrame(dataset1).toDF("id","name")
df2 = spark.createDataFrame(dataset2).toDF("id","name")


# COMMAND ----------

dfjoin = df1.join(df2.hint("broadcast"),"id","inner")
dfjoin.write.format("noop").mode("Overwrite").save("/Filestore")


# COMMAND ----------

dfjoin = df1.hint("Merge").join(df2,"id","inner")
dfjoin.write.format("noop").mode("Overwrite").save("/Filestore")

