-- Databricks SQL dashboard-ready queries for synthetic UPI fraud monitoring.

-- KPI 1: Total fraud volume
SELECT
  COUNT(*) AS total_fraud_transactions,
  ROUND(SUM(amount), 2) AS total_fraud_amount,
  ROUND(AVG(risk_score), 2) AS avg_fraud_risk
FROM gold.fraud_transactions;

-- KPI 2: Daily fraud trend
SELECT
  transaction_date,
  COUNT(*) AS fraud_count,
  ROUND(SUM(amount), 2) AS fraud_amount
FROM gold.fraud_transactions
GROUP BY transaction_date
ORDER BY transaction_date;

-- KPI 3: Merchant categories with highest fraud
SELECT
  merchant_category,
  COUNT(*) AS fraud_count,
  ROUND(SUM(amount), 2) AS fraud_amount
FROM gold.fraud_transactions
GROUP BY merchant_category
ORDER BY fraud_count DESC
LIMIT 10;

-- KPI 4: Mule account leaderboard
SELECT
  user_id,
  flagged_transaction_count,
  distinct_devices,
  distinct_cities,
  max_risk_score
FROM gold.mule_accounts
ORDER BY flagged_transaction_count DESC, max_risk_score DESC
LIMIT 20;

-- KPI 5: Fraud by city
SELECT
  location_city,
  COUNT(*) AS fraud_count,
  ROUND(SUM(amount), 2) AS fraud_amount
FROM gold.fraud_transactions
GROUP BY location_city
ORDER BY fraud_count DESC;

