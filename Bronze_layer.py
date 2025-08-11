# Databricks notebook source
# MAGIC %md
# MAGIC **** INCREMENTAL DATA INGESTION***

# COMMAND ----------

dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")
{src_value}

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/checking")\
  .option("cloudFiles.schemaEvolutionMode", "rescue")\
  .option("cloudFiles.maxFilesPerTrigger", 1)\
  .load(f"/Volumes/workspace/raw/rawvolume/rawdata/{src_value}/") 

# COMMAND ----------

def sanitize_col(col_name):
    return col_name.strip() \
                   .lower() \
                   .replace(" ", "_") \
                   .replace("(", "") \
                   .replace(")", "") \
                   .replace("{", "") \
                   .replace("}", "") \
                   .replace(";", "") \
                   .replace(",", "") \
                   .replace("=", "_")

df_clean = df.toDF(*[sanitize_col(c) for c in df.columns])


# COMMAND ----------

df_clean.writeStream.format("delta")\
    .trigger(once=True)\
    .outputMode("append")\
    .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/b{src_value}ank_detail/checkpoint")\
    .option("path", f"/Volumes/workspace/bronze/bronzevolume/{src_value}/data/")\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronzevolume/bank_detail/data/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/Volumes/workspace/bronze/bronzevolume/bank_detail/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/bronze/bronzevolume/bank_detail/data/`