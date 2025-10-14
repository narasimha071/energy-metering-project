# energy-metering-project
AWS-based time-series energy data pipeline with aggregation, billing &amp; anomaly detection.

## 📖 Overview
A cloud-based data pipeline that ingests, processes, and analyzes energy meter readings using AWS services.  
The project provides automated billing, anomaly detection, and visual analytics dashboards.

## Architecture
- **Ingestion:** AWS IoT Core → AWS Lambda → DynamoDB/S3  
- **Aggregation:** Databricks / AWS Glue (nightly ETL)  
- **Storage:** DynamoDB (raw) + S3 (aggregated)  
- **Analytics:** Athena + Power BI / QuickSight  
- **Alerts:** Lambda + SNS for anomalies

## Features
- Real-time data ingestion
- Daily/weekly aggregation
- REST APIs for billing
- Anomaly detection
- Cost-efficient partitioning
- Visualization dashboards

## Tech Stack
`AWS Lambda`, `DynamoDB`, `S3`, `Athena`, `Databricks`, `Power BI`, `Python`

## Outcomes
- Automated energy billing system  
- Scalable data storage & analytics  
- Real-time anomaly alerts  
- Energy efficiency insights

## 👨‍💻 Author
**Narasimha Raju**  
Bachelor of Technology – ECE | Gitam University, Hyderabad

