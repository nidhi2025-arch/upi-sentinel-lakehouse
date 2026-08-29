-- Synthetic Data only: analytical queries for fraud and mule detection.

-- 1. Top 10 high-risk users
SELECT
  user_id,
  COUNT(*) AS fraud_txn_count,
  SUM(amount) AS fraud_amount,
  MAX(risk_score) AS max_risk_score
FROM gold.fraud_transactions
GROUP BY user_id
ORDER BY max_risk_score DESC, fraud_amount DESC
LIMIT 10;

-- 2. Mule accounts
SELECT
  user_id,
  flagged_transaction_count,
  distinct_devices,
  distinct_cities,
  max_risk_score
FROM gold.mule_accounts
ORDER BY flagged_transaction_count DESC, max_risk_score DESC;

-- 3. Fraud by hour
SELECT
  HOUR(timestamp) AS txn_hour,
  COUNT(*) AS fraud_count,
  SUM(amount) AS fraud_amount
FROM gold.fraud_transactions
GROUP BY HOUR(timestamp)
ORDER BY txn_hour;

-- 4. Merchant fraud rate
SELECT
  merchant_category,
  COUNT(*) AS fraud_count,
  SUM(amount) AS fraud_amount,
  ROUND(AVG(risk_score), 2) AS avg_risk_score
FROM gold.fraud_transactions
GROUP BY merchant_category
ORDER BY fraud_count DESC, fraud_amount DESC;

-- 5. City-wise fraud
SELECT
  location_city,
  COUNT(*) AS fraud_count,
  SUM(amount) AS fraud_amount,
  COUNT(DISTINCT user_id) AS unique_users
FROM gold.fraud_transactions
GROUP BY location_city
ORDER BY fraud_count DESC, fraud_amount DESC;

