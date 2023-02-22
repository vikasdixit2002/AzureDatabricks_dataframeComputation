# Databricks notebook source
#Blob Storage using access key
blobstorage_account_name = "azstoragegen2"
blobstorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","gen2accesskey")
blobstorage_container_name = "golddb"

spark.conf.set(
    f"fs.azure.account.key.{blobstorage_account_name}.dfs.core.windows.net",
    blobstorage_account_accesskey)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists golddb;
# MAGIC 
# MAGIC drop table if exists golddb.employeeinfo;
# MAGIC 
# MAGIC create table golddb.employeeInfo(id int, val string);
# MAGIC 
# MAGIC insert into golddb.employeeinfo(id,val) values 
# MAGIC (1,'ralph1'),
# MAGIC (2,'ralph2');
# MAGIC 
# MAGIC 
# MAGIC Drop table if exists golddb.employeeInfo1;
# MAGIC create table golddb.employeeInfo1(id int, val string);
# MAGIC 
# MAGIC insert into golddb.employeeinfo1(id,val) values 
# MAGIC (1,'ralphtable11'),
# MAGIC (2,'ralphtable12'),
# MAGIC (3,'ralphtable13');

# COMMAND ----------

# MAGIC %python
# MAGIC id = 1
# MAGIC spark.conf.set("id.idval",id)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from golddb.employeeinfo where id = '${id.idval}'

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into golddb.employeeinfo src
# MAGIC using golddb.employeeinfo1 trgt
# MAGIC on src.id = trgt.id
# MAGIC WHEN MATCHED then  update set src.val = trgt.val
# MAGIC WHEN not MATCHED then insert  (id,val) values (trgt.id,trgt.val)

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into golddb.employeeinfo src
# MAGIC using golddb.employeeinfo1 trgt
# MAGIC on src.id = trgt.id
# MAGIC WHEN MATCHED and src.id = 1 then delete 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from golddb.employeeinfo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddb.employeeinfo1;

# COMMAND ----------

from delta import DeltaTable

d_tabletrgt = DeltaTable.forName(spark,"golddb.employeeinfo")
d_tablesrc = DeltaTable.forName(spark,"golddb.employeeinfo1")

d_tabletrgt.alias("src").merge(d_tablesrc.alias("trgt").toDF(),"src.id = trgt.id")\
                        .whenMatchedDelete()\
                        .execute()




# COMMAND ----------

from delta import DeltaTable

d_tabletrgt = DeltaTable.forName(spark,"golddb.employeeinfo")
d_tablesrc = DeltaTable.forName(spark,"golddb.employeeinfo1")

d_tabletrgt.alias("src").merge(d_tablesrc.alias("trgt").toDF(),"src.id = trgt.id")\
                        .whenMatchedUpdate(condition="src.id = 1",
                                          set = {"src.val" : "trgt.val"})\
                        .whenNotMatchedInsert(condition="trgt.id = 3", 
                                              values={
                                            "src.id":"trgt.id",
                                            "src.val":"trgt.val",
})\
                        .execute()




# COMMAND ----------

from delta import DeltaTable

d_tabletrgt = DeltaTable.forName(spark,"golddb.employeeinfo")
d_tablesrc = DeltaTable.forName(spark,"golddb.employeeinfo1")

d_tabletrgt.alias("src").merge(d_tablesrc.alias("trgt").toDF(),"src.id = trgt.id")\
                        .whenMatchedUpdateAll(condition="src.id = 1")\
                        .whenNotMatchedInsertAll(condition="trgt.id = 3")\
                        .execute()




# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from golddb.employeeinfo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from golddb.employeeinfo1;
