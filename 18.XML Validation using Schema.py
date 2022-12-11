# Databricks notebook source
spark.sparkContext.addFile("dbfs:/FileStore/XML/sample_order.xsd")
df_schema=spark.read.format("xml")\
           .option("rowTag","Root")\
           .option("rowValidationXSDPath","sample_order.xsd").load("/FileStore/XML/sample_corrupted.xml")

if (df_schema.columns[0]=="_corrupt_record"):
    dbutils.notebook.exit("Corrupted XML File")


# COMMAND ----------

spark.sparkContext.addFile("dbfs:/FileStore/XML/sample_order.xsd")
df_schema=spark.read.format("xml")\
           .option("rowTag","Root")\
           .option("rowValidationXSDPath","sample_order.xsd").load("/FileStore/XML/sample_order.xml")

if (df_schema.columns[0]=="_corrupt_record"):
    dbutils.notebook.exit("Corrupted XML File")
else:
    print("XML File parsed successfully")


# COMMAND ----------

display(df_schema)

# COMMAND ----------


