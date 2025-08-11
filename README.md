# databricks-banking-pipeline
Data engineering project with databricks
# Banking Fraud Detection Pipeline – Databricks Lakehouse

##  Project Overview
This project implements a **Bronze → Silver → Gold** data pipeline in **Databricks** for real-time banking fraud detection.  
The solution ingests raw banking transactions, processes them through multiple transformation stages, and applies machine learning to flag potentially fraudulent activities.

---

## Features
- **Incremental data ingestion** using Databricks **Auto Loader**
- **Delta Lake** storage architecture (Bronze, Silver, Gold)
- Multiple fraud detection signals:
  1. High transaction amount compared to customer’s average
  2. Sudden spike in transaction frequency
  3. Transactions from unusual countries
  4. Too many failed login attempts
- **Feature engineering** for ML model training
- **Fraud classification model** (Logistic Regression)
- Real-time anomaly detection
- Easy scalability for new fraud rules

---

## Architecture
