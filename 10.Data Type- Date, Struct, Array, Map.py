# Databricks notebook source
  spark.conf.get("spark.sql.session.timeZone")

# COMMAND ----------

import pyspark.sql.functions as sf

df = spark.read.format("json")\
            .load("/databricks-datasets/iot-stream/data-device/part-00000.json.gz")

df.printSchema()

#Changing String to Date
df1 = df.select(sf.to_date( sf.col("timestamP") ).alias("date"))
df1.printSchema()

#Changing String to Date
df2 = df.select(sf.to_timestamp( sf.col("timestamP") ).alias("TimeStamp"))
df2.printSchema()

#Changing Date Format
df3 = df2.select("*" ,sf.date_format(df2.TimeStamp,"d/M/y").alias("Newdateformat"))
df3.printSchema()

# COMMAND ----------

import pyspark.sql.functions as sf
import pyspark.sql.types as st

#Complex Type and Array
userschema = st.StructType([
                    st.StructField("FirstName",st.StringType(),True),
                    st.StructField("ID",st.LongType(),True),
                    st.StructField("LastName",st.StringType(),True),
                    st.StructField("Skills",st.ArrayType(st.StructType([
                                                        st.StructField("Skill",st.StringType(),True),
                                                        st.StructField("YearsOfExperience",st.StringType(),True)
                                                        ]))),
                    st.StructField("Address",st.StructType([
                                                        st.StructField("AddressLine1",st.StringType(),True),
                                                        st.StructField("AddressLine2",st.StringType(),True),
                                                        st.StructField("City",st.StringType(),True),
                                                        st.StructField("Country",st.StringType(),True)
                                                        ]))
                                   ])

df = spark.read.format("json")\
                .schema(userschema)\
               .load("/FileStore/tables/test.txt")
df_person =  df.select(df.ID,df.FirstName,df.LastName)

df_person_address =  df.select(df.ID,"Address.*")

df_ps=  df.select(df.ID,sf.explode(df.Skills).alias("Skills"))

df_person_skills =  df_ps.select("ID","Skills.*").where("Skill == 'SQL'")


display(df_person_skills)
display(df)

display(df_person)
display(df_person_address)


# COMMAND ----------

import pyspark.sql.functions as sf
import pyspark.sql.types as st

#Without Map type
userschema = st.StructType([
                    st.StructField("FirstName",st.StringType(),True),
                    st.StructField("ID",st.StringType(),True),
                    st.StructField("LastName",st.StringType(),True),
                    st.StructField("Skills",st.ArrayType(st.StructType([
                                                        st.StructField("Skill",st.StringType(),True),
                                                        st.StructField("YearsOfExperience",st.StringType(),True)
                                                        ]))),
                    st.StructField("Address",st.StructType([
                                                        st.StructField("AddressLine1",st.StringType(),True),
                                                        st.StructField("AddressLine2",st.StringType(),True),
                                                        st.StructField("City",st.StringType(),True),
                                                        st.StructField("Country",st.StringType(),True)
                                                        ])),
                     st.StructField("Contacts",st.StructType([
                                                        st.StructField("email",st.StringType(),True),
                                                        st.StructField("office",st.StringType(),True),
                                                        st.StructField("phone",st.StringType(),True),
                                                        st.StructField("whatsapp",st.StringType(),True)
                                                        ]))
                                   ])

df = spark.read.format("json")\
               .schema(userschema)\
               .load("/FileStore/tables/map_type_records.json")

df_contacts = df.select(df.ID,"Contacts.*")
display(df_contacts)

# COMMAND ----------

import pyspark.sql.functions as sf
import pyspark.sql.types as st

#Without Map type
userschema = st.StructType([
                    st.StructField("FirstName",st.StringType(),True),
                    st.StructField("ID",st.StringType(),True),
                    st.StructField("LastName",st.StringType(),True),
                    st.StructField("Skills",st.ArrayType(st.StructType([
                                                        st.StructField("Skill",st.StringType(),True),
                                                        st.StructField("YearsOfExperience",st.StringType(),True)
                                                        ]))),
                    st.StructField("Address",st.StructType([
                                                        st.StructField("AddressLine1",st.StringType(),True),
                                                        st.StructField("AddressLine2",st.StringType(),True),
                                                        st.StructField("City",st.StringType(),True),
                                                        st.StructField("Country",st.StringType(),True)
                                                        ])),
                     st.StructField("Contacts",st.MapType(st.StringType(),st.StringType(),True))
                                   ])

df = spark.read.format("json")\
               .schema(userschema)\
               .load("/FileStore/tables/map_type_records.json")

df1 = df.select(df.ID,sf.explode("Contacts"))

df2 = df.select(df.ID,df.Contacts.getItem("phone")
                     ,df.Contacts.getItem("email")
                     ,df.Contacts.getItem("office")
                     ,df.Contacts.getItem("whatsapp"))

df2 = df.select(df.ID,df.Contacts["phone"]
                     ,df.Contacts["email"]
                     ,df.Contacts["office"]
                     ,df.Contacts["whatsapp"])



# COMMAND ----------

dbutils.fs.head("/databricks-datasets/iot-stream/data-device/part-00001.json.gz")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/flights/")

# COMMAND ----------


