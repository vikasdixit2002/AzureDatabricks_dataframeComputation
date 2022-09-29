# Databricks notebook source
rows = [(1,"Vikas",1000),
       (1,"Vikas",1000),
       (2,"Ank",2000),
       (2,"Ank",3000),
       (3,"Paras",4000),
       (4,"Paras",4000)]

df = spark.createDataFrame(rows).toDF("id","name","salary")
df.show()


# COMMAND ----------

df_filtered = df.filter(df.name.like("%k%"))
df_filtered.show()

# COMMAND ----------

df_deleted = df.dropDuplicates(["id","name"])\
               .dropDuplicates(["name","salary"])
display(df_deleted)

# COMMAND ----------


