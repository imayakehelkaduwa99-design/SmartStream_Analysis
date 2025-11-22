CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.customer_cleaned` AS
SELECT
  -- Standardise ID and keep everything else
  CAST(Customer_ID AS INT64) AS customer_id,
  Chatbot_Usage_Count,
  SAFE_CAST(Last_Chatbot_Interaction AS DATE) AS last_chatbot_interaction,
  Email_Opened_Count,
  Clicked_Ad_Campaigns,
  Participated_in_Survey,
  Preferred_Channel,
  Loyalty_Program_Status,
  Marketing_Responsiveness,
  Referral_Likelihood,
  Gender,
  Tenure_Months
FROM
  `retail-analytics-478402.retail_project2.customer_raw`;


CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.transaction_cleaned` AS
SELECT
  Transaction_ID,
  CAST(Customer_ID AS INT64) AS customer_id,
  SAFE_CAST(Transaction_Date AS DATE) AS transaction_date,
  Product_SKU,
  Product_Description,
  Product_Category,
  CAST(Quantity AS INT64) AS quantity,
  CAST(Avg_Price AS NUMERIC) AS avg_price,
  CAST(Delivery_Charges AS NUMERIC) AS delivery_charges,
  Coupon_Status,
  Coupon_Code,
  CAST(Discount_pct AS NUMERIC) AS discount_pct,
  Payment_Method,
  Shipping_Provider,
  CAST(Transaction_Rating AS INT64) AS transaction_rating,
  -- derived metric: net revenue after discount
  CAST(Quantity AS INT64)
    * CAST(Avg_Price AS NUMERIC)
    * (1 - COALESCE(CAST(Discount_pct AS NUMERIC), 0) / 100.0) AS net_revenue
FROM
  `retail-analytics-478402.retail_project2.transaction_raw`;

  -- Q1 – product-level KPIs for 2024
CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.product_performance_2024` AS
SELECT
  Product_SKU,
  Product_Description,
  Product_Category,
  SUM(Quantity) AS total_units_sold,
  COUNT(DISTINCT Transaction_ID) AS num_transactions,
  -- Revenue excluding shipping, after discount
  SUM(
    Quantity * Avg_Price * (1 - COALESCE(Discount_pct, 0) / 100.0)
  ) AS total_revenue,
  AVG(Transaction_Rating) AS avg_transaction_rating
FROM
  `retail-analytics-478402.retail_project2.transaction_cleaned`
WHERE
  EXTRACT(YEAR FROM transaction_date) = 2024
GROUP BY
  Product_SKU, Product_Description, Product_Category;

-- Top 10 most popular products (by quantity sold)
SELECT
  *
FROM
  `retail-analytics-478402.retail_project2.product_performance_2024`
ORDER BY
  total_units_sold DESC
LIMIT 10;

-- Products customers “favour” most (high rating, non-tiny sales)
SELECT
  *
FROM
  `retail-analytics-478402.retail_project2.product_performance_2024`
WHERE
  total_units_sold >= 20      -- adjust threshold if needed
ORDER BY
  avg_transaction_rating DESC,
  total_units_sold DESC
LIMIT 10;

-- Q2 – calculate raw Recency, Frequency, Monetary per customer
CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.customer_rfm_base` AS
WITH customer_tx AS (
  SELECT
    customer_id,
    MAX(transaction_date) AS last_purchase_date,
    COUNT(DISTINCT Transaction_ID) AS frequency,
    SUM(
      Quantity * Avg_Price * (1 - COALESCE(Discount_pct, 0) / 100.0)
    ) AS monetary
  FROM
    `retail-analytics-478402.retail_project2.transaction_cleaned`
  GROUP BY
    customer_id
)
SELECT
  customer_id,
  last_purchase_date,
  -- recency in days: smaller = more recent
  DATE_DIFF(
    (SELECT MAX(transaction_date)
     FROM `retail-analytics-478402.retail_project2.transaction_cleaned`),
    last_purchase_date,
    DAY
  ) AS recency_days,
  frequency,
  monetary
FROM
  customer_tx;

  -- Q3 – RFM quintiles and combined RFM code
CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.customer_rfm_scored` AS
WITH base AS (
  SELECT
    *
  FROM
    `retail-analytics-478402.retail_project2.customer_rfm_base`
),
scored AS (
  SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,

    -- Recency: more recent (smaller days) should get higher score,
    -- so we compute NTILE ascending then reverse it.
    6 - NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,

    -- Frequency: more orders = higher score
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,

    -- Monetary: more spending = higher score
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
  FROM
    base
)
SELECT
  customer_id,
  recency_days,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CONCAT(CAST(r_score AS STRING),
         CAST(f_score AS STRING),
         CAST(m_score AS STRING)) AS rfm_code
FROM
  scored
ORDER BY
  rfm_code DESC,
  monetary DESC;

-- Helper table: bring together marketing attributes + spend per customer
CREATE OR REPLACE TABLE
  `retail-analytics-478402.retail_project2.customer_marketing_value` AS
SELECT
  c.customer_id,
  c.Preferred_Channel,
  c.Loyalty_Program_Status,
  c.Marketing_Responsiveness,
  c.Referral_Likelihood,
  c.Chatbot_Usage_Count,
  c.Email_Opened_Count,
  c.Clicked_Ad_Campaigns,
  c.Participated_in_Survey,
  c.Tenure_Months,
  COUNT(DISTINCT t.Transaction_ID) AS order_count,
  SUM(
    t.Quantity * t.Avg_Price * (1 - COALESCE(t.Discount_pct, 0) / 100.0)
  ) AS total_revenue,
  AVG(
    t.Quantity * t.Avg_Price * (1 - COALESCE(t.Discount_pct, 0) / 100.0)
  ) AS avg_order_value
FROM
  `retail-analytics-478402.retail_project2.customer_cleaned` c
LEFT JOIN
  `retail-analytics-478402.retail_project2.transaction_cleaned` t
ON
  c.customer_id = t.Customer_ID
GROUP BY
  c.customer_id,
  c.Preferred_Channel,
  c.Loyalty_Program_Status,
  c.Marketing_Responsiveness,
  c.Referral_Likelihood,
  c.Chatbot_Usage_Count,
  c.Email_Opened_Count,
  c.Clicked_Ad_Campaigns,
  c.Participated_in_Survey,
  c.Tenure_Months;

-- Which channel brings the highest-spending customers?
SELECT
  Preferred_Channel,
  COUNT(*) AS num_customers,
  SUM(total_revenue) AS channel_revenue,
  AVG(total_revenue) AS avg_revenue_per_customer,
  AVG(order_count) AS avg_orders_per_customer
FROM
  `retail-analytics-478402.retail_project2.customer_marketing_value`
GROUP BY
  Preferred_Channel
ORDER BY
  avg_revenue_per_customer DESC;
