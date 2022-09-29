# Databricks notebook source
# MAGIC %md
# MAGIC Spark Handling Null Joins

# COMMAND ----------



emp_dataset = [(1, "Vikas1",11),
           (2, "Vikas2",12),
           (3, "Vikas3",13),
           (4, "Vikas4",None),
           (5, "Vikas5",None)
]

dept_dataset = [
(11,"HR"),
(12,"Finance"),
(13,"Product"),
]

emp_df = spark.createDataFrame(emp_dataset).toDF("emp_id","emp_name","dept_id")
dept_df = spark.createDataFrame(dept_dataset).toDF("dept_id","dept_name")

emp_dept_df = emp_df.join(dept_df,"dept_id").select(emp_df.emp_id,emp_df.emp_name,dept_df.dept_name)
display(emp_dept_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Null Filtering

# COMMAND ----------

emp_df_nullfilter = emp_df.where(emp_df.dept_id.isNotNull())
display(emp_df_nullfilter)



# COMMAND ----------

from pyspark.sql.functions import expr
emp_df_nullfilterexpr = emp_df.where(expr("dept_id is not null "))
display(emp_df_nullfilterexpr)

# COMMAND ----------

# MAGIC %md
# MAGIC NA functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

songs_schema =StructType([
    StructField("id",StringType(),True),
    StructField("log",DoubleType(),True),
    StructField("lat",DoubleType(),True),
    StructField("location",StringType(),True),
    StructField("songs",StringType(),True)
]) 
df1 = spark.read.format("csv")\
                 .option("sep","\t")\
                 .schema(songs_schema)\
                .load("/databricks-datasets/songs/data-001/part-00000") 
#NA Drop
print(df1.na.drop().count())
print(df1.where(df1.id.isNotNull() & df1.log.isNotNull() & df1.lat.isNotNull() & df1.location.isNotNull() & df1.songs.isNotNull()).count() )




# COMMAND ----------

# MAGIC %fs ls "/databricks-datasets/credit-card-fraud/data/_committed_7254527126265311508/"

# COMMAND ----------

# MAGIC %fs ls "/databricks-datasets/songs/data-001/part-00000"

# COMMAND ----------

 dbutils.fs.head("/databricks-datasets/credit-card-fraud/data/_committed_7254527126265311508/")

# COMMAND ----------


