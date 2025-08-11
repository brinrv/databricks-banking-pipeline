# Databricks notebook source
src_array = [
    {"src": "account"},
    {"src": "bank_details"},
    {"src": "customer"},
    {"src": "transactions"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key",value =  src_array)