# Databricks notebook source
#Blob Storage using access key
gen2storage_account_name = "azstoragegen2"
gen2storage_account_accesskey =  dbutils.secrets.get("azdb_secretscope","gen2accesskey")
gen2storage_container_name = "inbound"
mountpointname = "/mnt/azdbcontainergen2_point"

dbutils.fs.mount(source = f"wasbs://{gen2storage_container_name}@{gen2storage_account_name}.blob.core.windows.net",
                    mount_point = mountpointname,
                     extra_configs = {"fs.azure.account.key."+gen2storage_account_name+".blob.core.windows.net":gen2storage_account_accesskey})

