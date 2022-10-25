# Databricks notebook source
import pyspark.sql.functions as sf

# COMMAND ----------

# MAGIC %fs ls /FileStore/vikasd/flight_time.avro

# COMMAND ----------

# MAGIC %md
# MAGIC Get default number of Partition

# COMMAND ----------

df = spark.read.format("avro")\
          .load("/FileStore/vikasd/flight_time.avro")
df.rdd.getNumPartitions()


# COMMAND ----------

# MAGIC %md
# MAGIC In Memory Partition - Change number of Partition

# COMMAND ----------

df1 = df.repartition(5)
df1.rdd.getNumPartitions()

# COMMAND ----------

df2 = df.repartition("FL_date")
df2.rdd.getNumPartitions()

# COMMAND ----------

df3 = df.repartitionByRange(15,"FL_date")
df3.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Finding partitioning by partition ID

# COMMAND ----------

df4 = df3.select(sf.spark_partition_id().alias("PartitionID"),"*")
display(df4.select("PartitionID").distinct() )

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Writing to DBFS

# COMMAND ----------

df.write.format("avro")\
        .save("/FileStore/vikasd/flight_time.avroWrite")

# COMMAND ----------

# MAGIC %fs ls "/FileStore/vikasd/flight_time.avroWrite"

# COMMAND ----------

# MAGIC %md
# MAGIC Changing Number of partitions while writing

# COMMAND ----------

df.write.format("avro")\
        .partitionBy("FL_date","OP_CARRIER")\
        .save("/FileStore/vikasd/flight_time.avroWritePartition")

# COMMAND ----------

# MAGIC %fs ls "/FileStore/vikasd/flight_time.avroWritePartition"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading from partitioned files

# COMMAND ----------

partitioned_df = spark.read.format("avro")\
          .load("/FileStore/vikasd/flight_time.avroWritePartition")
partitioned_df.rdd.getNumPartitions()


# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.get("spark.default.parallelism")

# COMMAND ----------


