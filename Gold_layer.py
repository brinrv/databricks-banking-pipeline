# Databricks notebook source
# DBTITLE 1,Gold layer tables
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id

# GOLD Paths
gold_path = "/Volumes/workspace/gold/goldvolume"
silver_path = "/Volumes/workspace/silver/silvervolume"

# Load Silver Layer tables
stg_customers = spark.read.format("delta").load(f"{silver_path}/stg_customers")
stg_accounts  = spark.read.format("delta").load(f"{silver_path}/stg_accounts")
stg_txns      = spark.read.format("delta").load(f"{silver_path}/stg_transactions")

# ✅ Join Customers, Accounts & Transactions → Base Fact Table
fact_txns = (
    stg_txns.alias("t")
    .join(stg_accounts.alias("a"), col("t.account_id") == col("a.account_id"), "left")
    .join(stg_customers.alias("c"), col("a.customer_id") == col("c.customer_id"), "left")
    .select(
        "t.transaction_id",
        "t.account_id",
        "t.txn_date",
        "t.amount",
        "t.txn_type",
        "a.account_type",
        "a.status",
        "c.customer_id",
        "c.dob",
        "c.risk_category"
    )
)

# ✅ Feature Engineering for Fraud
from pyspark.sql.functions import avg, stddev, count, when

# Transaction-level aggregates per account
txn_features = (
    fact_txns.groupBy("account_id")
    .agg(
        avg("amount").alias("avg_txn_amount"),
        stddev("amount").alias("stddev_txn_amount"),
        count("transaction_id").alias("txn_count")
    )
)

# Merge back with original transactions
gold_txns = (
    fact_txns.join(txn_features, "account_id", "left")
    .withColumn(
        "is_high_value",
        when(col("amount") > (col("avg_txn_amount") + 2 * col("stddev_txn_amount")), 1).otherwise(0)
    )
)

# ✅ Save Gold Features
gold_txns.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/fact_transactions_gold")

print("✅ Gold Layer Created with fraud features!")


# COMMAND ----------

display(stg_customers)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_txns limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`/Volumes/workspace/gold/goldvolume/fact_transactions_gold/`;