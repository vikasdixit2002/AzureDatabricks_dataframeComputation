# Databricks notebook source
from pyspark.sql.functions import expr

# COMMAND ----------

rows = [
    (1,"Vikas1"),
    (2, "Vikas2"),
    (3,"Vikas3")    
]

df = spark.createDataFrame(rows).toDF("id","name")
display(df)

# COMMAND ----------

df_withSalary = df.withColumn("Salary",expr("id*1000"))\
                  .withColumn("Increment",expr("Salary*.1"))

display(df_withSalary)

# COMMAND ----------

df_withSelectSalary = df.select(df.id,df.name,expr("id*1000 as Salary"), expr("id*1000*.1 as Increment"))
display(df_withSelectSalary)

# COMMAND ----------

df_withSelectSalary.drop(df_withSelectSalary.Increment)
df_withSelectSalary.show(5)

# COMMAND ----------


