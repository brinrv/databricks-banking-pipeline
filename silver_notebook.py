# Databricks notebook source
# DBTITLE 1,correct silver notebook
from pyspark.sql.functions import col, trim, lower, to_date, regexp_replace
from pyspark.sql.functions import coalesce, to_date, lower, trim
from pyspark.sql.functions import expr, col, coalesce  # ✅ import expr

# ✅ Reusable function for multiple date formats
def parse_multi_date(col_name):
    return coalesce(
        expr(f"try_to_date({col_name}, 'dd-MM-yyyy')"),   # e.g. 22-01-2008
        expr(f"try_to_date({col_name}, 'yyyy-MM-dd')")    # e.g. 2008-01-22
    )


# Paths for Bronze & Silver
bronze_path = "/Volumes/workspace/bronze/bronzevolume"
silver_path = "/Volumes/workspace/silver/silvervolume"

# -----------------------------
# 1️⃣ READ BRONZE DATA
# -----------------------------
bronze_customers_df = spark.read.format("delta").load(f"{bronze_path}/customer/data")
bronze_accounts_df  = spark.read.format("delta").load(f"{bronze_path}/account/data")
bronze_txn_df       = spark.read.format("delta").load(f"{bronze_path}/transactions/data")
bronze_clients_df   = spark.read.format("delta").load(f"{bronze_path}/bank_detail/data")

# -----------------------------
# 2️⃣ CLEAN COLUMN NAMES
# -----------------------------
def clean_colnames(df):
    return df.toDF(*[c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns])

customers_df = clean_colnames(bronze_customers_df)
accounts_df  = clean_colnames(bronze_accounts_df)
txn_df       = clean_colnames(bronze_txn_df)
clients_df   = clean_colnames(bronze_clients_df)

from pyspark.sql.functions import to_date, coalesce

def parse_multi_date(df, col_name):
    return df.withColumn(
        col_name,
        coalesce(
            to_date(col_name, "dd-MM-yyyy"),   # first try dd-MM-yyyy
            to_date(col_name, "yyyy-MM-dd")    # fallback yyyy-MM-dd
        )
    )



#✅ Customers: parse dates & lower risk_category
customers_df = (
    customers_df
    .dropDuplicates(["customer_id"])
    .withColumn("dob", to_date(col("dob"), "dd-MM-yyyy"))
    .withColumn("updated_at", to_date(col("updated_at"), "dd-MM-yyyy"))
    .withColumn("risk_category", lower(trim(col("risk_category"))))
    .withColumn("email", lower(trim(col("email"))))
)
customers_df.show()
#Accounts: parse opened_at
accounts_df = (
    accounts_df
    .dropDuplicates(["account_id"])
    .withColumn(
        "opened_at",
        coalesce(
            to_date(col("opened_at"), "yyyy-MM-dd"),
            to_date(col("opened_at"), "yyyy-MM-dd")))
    
    .withColumn("account_type", lower(trim(col("account_type"))))
    .withColumn("status", lower(trim(col("status"))))
)
display(accounts_df)
accounts_df.show()
# # ✅ Transactions: parse txn_date & amount
txn_df = (
    txn_df
    .dropDuplicates(["transaction_id"])
    .withColumn(
        "txn_date",
        coalesce(
            to_date(col("txn_date"), "yyyy-MM-dd"),
            to_date(col("txn_date"), "yyyy-MM-dd")))

    .withColumn("txn_type", lower(trim(col("txn_type"))))
    .withColumn("amount", regexp_replace(col("amount"), ",", "").cast("double"))
)

# -----------------------------
# 4️⃣ WRITE TO SILVER LAYER
# -----------------------------
customers_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}/stg_customers")
accounts_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}/stg_accounts")
txn_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}/stg_transactions")
# clients_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}/stg_clients")

print("✅ Silver Layer completed: stg_customers, stg_accounts, stg_transactions, stg_clients created.")

# COMMAND ----------

# DBTITLE 1,Silver client table
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType
from datetime import datetime

# ✅ Safe multi-format parser
def safe_parse_date(date_str):
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None  # If all formats fail, return NULL

# ✅ Register it as UDF
safe_parse_date_udf = udf(safe_parse_date, DateType())

# ✅ Apply it safely
clients_df = (
    clients_df
    .dropDuplicates(["client_id"])
    .withColumn("joined_bank", safe_parse_date_udf(col("joined_bank")))
    .withColumn("gender", lower(trim(col("gender"))))
    .withColumn("country", lower(trim(col("country")))
            )
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`/Volumes/workspace/bronze/bronzevolume/customer/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`/Volumes/workspace/bronze/bronzevolume/account/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`/Volumes/workspace/silver/silvervolume/stg_customers/`