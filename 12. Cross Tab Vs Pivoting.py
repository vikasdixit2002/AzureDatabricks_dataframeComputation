# Databricks notebook source
import pyspark.sql.functions as sf
import pyspark.sql.types as st



# COMMAND ----------

tableschema = st.StructType([
    st.StructField("CustomerName",st.StringType(), True)
    ,st.StructField("Product",st.StringType(), True)
    ,st.StructField("Quantity",st.LongType(), True)
    ,st.StructField("UnitPrice",st.DoubleType(), True)
])

df = spark.read.format("csv")\
               .schema(tableschema)\
               .load("/FileStore/tables/sales")


# COMMAND ----------

# MAGIC %md
# MAGIC Cross tab shows total number of occurrences

# COMMAND ----------

df.crosstab("CustomerName","Product").show()
df.where("customername == 'Mike' and Product= 'apples'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Pivoting table

# COMMAND ----------

df.groupBy("CustomerName")\
  .pivot("Product")\
  .agg(sf.sum(sf.expr("Quantity * UnitPrice"))).show()

# COMMAND ----------


