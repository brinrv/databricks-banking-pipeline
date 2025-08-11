# Databricks notebook source
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Assume `gold_txns` also has a `fraud_label` column (1 = fraud, 0 = normal)
train_df, test_df = gold_txns.randomSplit([0.8, 0.2], seed=42)

# Features for ML
feature_cols = ["amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_ml = assembler.transform(train_df).select("features", "fraud_label")
test_ml  = assembler.transform(test_df).select("features", "fraud_label")

# Train Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="fraud_label")
model = lr.fit(train_ml)

# Evaluate
predictions = model.transform(test_ml)
evaluator = BinaryClassificationEvaluator(labelCol="fraud_label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"✅ Fraud Detection Model AUC: {auc}")


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# ✅ Step 1: Load a manageable sample
gold_txns = (
    spark.read.format("delta").load(f"{gold_path}/fact_transactions_gold")
    .sample(fraction=0.2, seed=42)  # ✅ sample only 20% for free tier
)

# ✅ Step 2: Assemble features for MLlib
feature_cols = ["amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

ml_df = assembler.transform(gold_txns).select("features", "fraud_label")

# ✅ Step 3: Train/Test split
train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

# ✅ Step 4: Train Logistic Regression (Spark ML)
lr = LogisticRegression(labelCol="fraud_label", featuresCol="features", maxIter=50)
model = lr.fit(train_df)

# ✅ Step 5: Predict & Evaluate
predictions = model.transform(test_df)

evaluator = BinaryClassificationEvaluator(
    labelCol="fraud_label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)
auc = evaluator.evaluate(predictions)
print(f"✅ Spark ML Fraud Detection AUC: {auc:.4f}")

# ✅ Step 6: Save Predictions to Gold Layer
predictions.write.format("delta").mode("overwrite").save(f"{gold_path}/fraud_predictions")


# COMMAND ----------

result = spark.sql("""
    SELECT COUNT(1) 
    FROM delta.`/Volumes/workspace/gold/goldvolume/fact_transactions_gold/`
""")
try:    display(result)
except Exception as e:
    import time
    time.sleep(5)
    display(result)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM delta.`/Volumes/workspace/gold/goldvolume/fact_transactions_gold/`

# COMMAND ----------

silver_path = "/Volumes/workspace/silver/silvervolume"
gold_path   = "/Volumes/workspace/gold/goldvolume"

# gold_txns = spark.read.format("delta").load(f"{gold_path}/fact_transactions_gold")

# Get Gold features into Pandas
import pandas as pd

# ✅ Step 1: Load a small Gold sample
gold_txns = (
    spark.read.format("delta")
    .load(f"{gold_path}/fact_transactions_gold")
    .limit(500)
    .sample(fraction=0.1, seed=42)  # only 10% sample
)
#gold_txns.columns = gold_txns.columns.str.lower()
# ✅ Step 2: Convert to Pandas for sklearn
gold_pdf = gold_txns.select(
    "amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value", "fraud_label"
).toPandas()

print(f"✅ Loaded {len(gold_pdf)} rows for ML")

# ✅ Step 3: Split features & labels
X = gold_pdf[["amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value"]]
y = gold_pdf["fraud_label"]

# ✅ Step 4: Train/Test Split
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ✅ Step 5: Train Logistic Regression
model = LogisticRegression(max_iter=200)
model.fit(X_train, y_train)

# ✅ Step 6: Evaluate
y_pred_prob = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_prob)

print(f"✅ Fraud Detection Model (sklearn) AUC: {auc:.4f}")


# COMMAND ----------

import pandas as pd
from pyspark.sql.utils import AnalysisException
from time import sleep

silver_path = "/Volumes/workspace/silver/silvervolume"
gold_path   = "/Volumes/workspace/gold/goldvolume"

# Function to load data with retries
def load_data_with_retries(path, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return spark.read.format("delta").load(path)
        except AnalysisException as e:
            if attempt < retries - 1:
                sleep(delay)
                continue
            else:
                raise e

# Load a small Gold sample
gold_txns = load_data_with_retries(f"{gold_path}/fact_transactions_gold").sample(fraction=0.1, seed=42)

# Convert to Pandas for sklearn
gold_pdf = gold_txns.select(
    "amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value", "fraud_label"
).toPandas()

print(f"✅ Loaded {len(gold_pdf)} rows for ML")

# Split features & labels
X = gold_pdf[["amount", "txn_count", "avg_txn_amount", "stddev_txn_amount", "is_high_value"]]
y = gold_pdf["fraud_label"]

# Train/Test Split
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Logistic Regression
model = LogisticRegression(max_iter=200)
model.fit(X_train, y_train)

# Evaluate
y_pred_prob = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_prob)


print(f"✅ Fraud Detection Model (sklearn) AUC: {auc:.4f}")

# COMMAND ----------

silver_path = "/Volumes/workspace/silver/silvervolume"
gold_path   = "/Volumes/workspace/gold/goldvolume"

# Load table from Hive Metastore / Delta Table
import pandas as pd

# ✅ Step 1: Load a small Gold sample
gold_txns = (
    spark.read.format("delta")
    .load(f"{gold_path}/fact_transactions_gold")
    .limit(500)
    .sample(fraction=0.1, seed=42)  # only 10% sample
)
# display(gold_txns)


# COMMAND ----------

df = gold_txns.toPandas()
df.columns = df.columns.str.lower()
print(df.head())


# COMMAND ----------

df = gold_txns.limit(1000).toPandas()
df.columns = df.columns.str.lower()
display(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC