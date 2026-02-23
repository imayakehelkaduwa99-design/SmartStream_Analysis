# SmartStream Analysis - SQL Analytics Case Study
**Project Overview**
This project is an industry-style analytics case study for a fictional digital retailer, SmartStream, focused on understanding **product performance, customer value, and marketing effectiveness using SQL in Google BigQuery**.

The analysis uses two core datasets - **customer data** and **transactional data** - to build clean analytical tables, compute business KPIs, and answer commercially relevant questions such as:
1. Which products perform best?
2. Who are our most valuable customers?
3. How strong is customer loyalty and engagement?
4. Which marketing channels drive higher-value customers?

The project demonstrates **end-to-end SQL analytics: data cleaning, transformation, feature engineering, KPI creation, and business-oriented analysis**.

**Business Problem / Case Context**
SmartStream operates in a competitive digital retail environment and needs better visibility into:
- Product performance and customer preferences
- Customer purchasing behaviour and loyalty
- The relationship between marketing channels and customer value

The business wants **decision-ready insights** to support:
- Product portfolio optimisation
- Customer segmentation and retention strategies
- Marketing channel investment decisions

**Data Sources**
The data was provided via an academic-industry style brief and represents a realistic retail environment.

**Core Datasets**
- **Customer Data**
  Contains customer attributes such as:
  Preferred channel
  Loyalty status
  Marketing responsiveness
  Engagement metrics (email opens, chatbot usage, ad clicks, surveys)
  Tenure and demographics

- **Transaction Data**
  Contains transactional records including:
  Transaction date
  Product details (SKU, category, description)
  Quantity and pricing
  Discounts and delivery charges
  Payment and shipping details
  Transaction ratings

The data is used exclusively in **Goolge BigQuery** for cleaning, transformation, and analysis.

**Data Pipeline (BigQuery)**
1. **Raw tables ingested** into BigQuery:
   customer_raw
   transaction_raw

2. **Cleaned analytical tables created**:
   customer_cleaned
   transaction_cleaned
   Standardised data types
   Parsed dates
   Derived metric: net_reveue (after discount)

3. **Analytical feature tables built:**
   product_performance_2024
   customer_rfm_base
   customer_rfm_scored
   customer_marketing_value

4. **Final analytical queries** answer business questions and generate outputs exported to CSV/Excel.

This mirrors a **real-world analytics workflow: raw → cleaned → feature tables → business outputs.

**Analytics & SQL Methodology**
The project uses SQL to:
- Clean and standardise customer and transaction data
- Derive revenue metrics adjusted for discounts
- Aggregate product performance metrics for 2024
- Build **RFM (Recency, Frequency, Monetary)** customer features
- Score customers using **quintile-based RFM segmentation**
- Combine behavioural + transactional data into a **customer marketing value table**
- Evaluate marketing channels by customer value and order behaviour

All logic is implemented in **BigQuery SQL** using:
- CREATE OR REPLACE TABLE pipelines
- Window functions (NTILE) for RFM scoring
- Aggregations for revenue, frequency, and product KPIs
- Date functions for recency calculations

**Business Questions Answered**
1. Which products perform best in terms of:
   Units sold
   Revenue
   Transaction volume
   Customer ratings

2. Which products are:
   Most popular (by volume)
   Most favoured (high ratings with meaningful sales)

3. How can customers be segmented using **RFM analysis**?
4. Who are the most valuable customers based on:
   Total revenue
   Order frequency
   Recency of purchase

5. Which marketing channels are associated with:
   Higher-spending customers
   Higher order frequency
   Better average customer value

**KPIs Produced / Analysed**
**Product KPIs**
- Total units sold
- Number of transactions
- Total revenue (net of discounts)
- Average transaction rating

**Customer KPIs**
- Recency (days since last purchase)
- Frequency (number of orders)
- Monetary value (total spend)
- RFM scores and RFM code

**Marketing & Commercial KPIs**
- Total revenue per customer
- Average order value
- Order count per customer
- Revenue and value by marketing channel

**Key Outputs (in this Repository)**
The following outputs are generated from the SQL and included in the repo:
- Product Performance 2024
  Product Performance 2024.csv

- Most Popular Products
  most popular products.csv

- Most favoured products (high rating + meaningful sales)
  most favoured products.csv

- Customer RFM Tables
  Customer RFM Base.csv
  Customer RFM Scored.csv

- Customer Marketing Value Table
  Customer Marketing Value.csv

- Marketing Channel Performance
  most popular marketing channels.csv

These represent analysis-ready business outputs, not just raw query results. Refer to the Insights Report document on the main branch  for further analysis on these tables. 

**Insights**
From the analysis, the business can:
- Identify top-performing products by both **volume and revenue**
- Distinguish between:
  Products that sell a lot vs. products customers rate highly
- Segment customers by **value and engagement** using RFM scoring
- Understand which marketing channels are associated with:
  Higher customer lifetime value
  Higher order frequency
- Prioritise customers and products based on measurable commercial impact

**Recommendations**
- Use **RFM segments** to support targeted retention and reactivation campaigns
- Prioritise **high-performing and highly-rated products** in merchandising and promotions
- Review **marketing channel performance** to guide budget allocation toward higher-value customer sources
- Monitor **customer value metrics over time** to track changes in loyalty and engagement
- Use the existing SQL pipeline as a foundation for **ongoing KPI reporting**

These recommendations are intended to support **evidence-based commerical decision-making** rather than speculative optimisation.

**Repository Structure**
- SmartStream_Analysis_Query.sql
  Full end-to-end SQL script containing the complete pipeline and analysis logic
- /sql and /sql_subqueries
  Modular SQL queries broken down by business question and output (e.g., product performance, RFM, marketing analysis)
- Customer Data_Raw*, Transaction Data_Raw*
  Original raw datasets
- Customer Data_Cleaned.xlsx, Transaction Data_Cleaned.xlsx
  Cleaned versions used for analysis
- Output CSV/Excel files
  Final analytical results exported from BigQuery
- README.md
  Business and technical documentation for the project

This structure mirrors how **analytics teams separate raw data, transformations, and business outputs** in real projects. 

**Tools Used**
1. **Google BigQuery** - SQL engine for data cleaning, transformation, and analysis
2. **SQL** - Core language for analytics and feature engineering
3. **Excel/CSV** - For exporting and reviewing final outputs

**Why This Project Matters**
This project demonstrates:
1. End-to-end SQL analytics workflow
2. Data modelling and feature engineering
3. Business KPI development
4. Customer segmentation (RFM)
5. Product and marketing performance analysis
6. Translating raw data into **decision-ready business insights**

It reflects the type of work done in **commercial analytics, BI, and data analyst roles** rather than academic exercises.

**Author: Imaya Kehelkaduwa (Analytics & Data Engineering Portfolio)**
