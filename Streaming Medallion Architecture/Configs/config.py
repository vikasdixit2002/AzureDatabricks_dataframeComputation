# Databricks notebook source
# Bronze layer variables
bronze_notebook = "2. Bronze Layer -AdlsGen2_Ingestion_Autoloader" 
bronze_layer = "Bronze"

bronzestorage_account_name = "azstoragegen2"
bronzestorage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","gen2accesskey")
bronzestorage_container_name = "source"

# COMMAND ----------

# Silver layer variable
silver_notebook = "3. Silver Layer-DataCleansing_Transformation"
silver_layer = "Silver"

# COMMAND ----------

# Gold layer variable
gold_notebook = "4. Gold Layer-Data Aggregation"
gold_layer = "Gold"
