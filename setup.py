# Databricks notebook source
# MAGIC %sql
# MAGIC create volume workspace.raw.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume workspace.bronze.bronzevolume
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/raw/bronzevolume/")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/rawdata/bank_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema workspace.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/bank_details/data/`