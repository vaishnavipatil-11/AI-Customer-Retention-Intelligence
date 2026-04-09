📊 AI-Customer-Retention-Intelligence
AI-powered customer retention system that builds dynamic health scoring, segments users, and generates personalized LLM-based email campaigns. End-to-end cloud pipeline using Python, BigQuery, and Power BI (DirectQuery), designed to demonstrate real-world analytics, automation, and business impact.
AI Customer Retention Intelligence

🔗 Core Dataset

The base dataset is the SaaS Subscription & Churn Analytics Dataset from Kaggle, which provides real-world user activities and engagement signals.
Original dataset reference:
🔗 [https://www.kaggle.com/datasets/rivalytics/saas-subscription-and-churn-analytics-dataset]

The dataset is extended using a Python simulation script to mimic real daily user activity patterns.

🧠 Pipeline Overview
🟡 1. Data Simulation (Python)
Python scripts generate simulated SaaS user behavior on a daily basis.
This includes activity logs, sentiment signals, usage durations, and engagement events.
Simulated data is loaded into BigQuery as raw input for analytics.

🟢 2. Data Warehouse (BigQuery)
Using Google BigQuery as the analytical warehouse:
SQL Transformations:
Health Score Calculation
Health Score = User Sentiment Score * 0.4 + Tenure Score * 0.4 + Usage Score * 0.2
Designed to capture user experience, duration with product, and actual resource usage.
Segmentation of Users
Based on the Health Score, users are classified into:
Healthy
Warning
Critical
Customer Health Matrix View
A final analytical view combining user segments with key metrics.
Customer Health History Snapshot
Historical snapshots stored to analyze trends over time (e.g., movement between health segments).

🤖 3. Personalized Email Creation (LLM Integration)
Integrated Gemini Flash 2.5 to generate personalized retention email messages for Critical users.
Emails are tailored based on user engagement signals and segment context.
This supports marketing teams in taking quick, targeted retention actions.

📊 4. Power BI Dashboard (DirectQuery)
A live dashboard using Power BI with DirectQuery connection to BigQuery:

Key Visualizations:

Monthly Recurring Revenue (MRR)
Churned MRR
Total Churn Count
Retention Actions Triggered (Emails Generated)
Health Score Trends
Segmentation Breakdown

DirectQuery ensures the latest data is reflected live in the dashboard without manual refresh exports.

🛠 Tech Stack
Layer	Technology
Data Simulation	Python
Data Warehouse	Google BigQuery
BI Visualization	Power BI (DirectQuery)
LLM Personalization	Gemini Flash 2.5
Source Dataset	Kaggle User Engagement Data

🎯 What This Project Demonstrates
Real-world data pipeline integration
Cloud analytics architecture design
Advanced scoring and segmentation logic
Analytical modeling for retention insights
LLM-driven personalized content generation
Live BI reporting for business intelligence

🧩 How to Run
Create a BigQuery dataset and upload initial data.
Run the Python script to populate simulated activity.
Deploy SQL transformation logic to compute scores and snapshots.
Connect Power BI to BigQuery via DirectQuery.
Configure the LLM integration with environment variable API key

📁 Repo Structure

AI-Customer-Retention-Intelligence/
├── README.md
├── architecture.png
├── python/
│   └── data_simulation.py
├── sql/
│   └── transformations.sql
├── email_generation/
│   └── llm_email_generator.py
├── powerbi/
│   └── dashboard.pbix
└── sample_data/
    └── ravenstack_engagement_sample.csv
