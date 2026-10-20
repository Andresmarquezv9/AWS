# Serverless Data Lake & Medallion Architecture on AWS

## Overview
This repository contains an end-to-end Data Engineering project demonstrating a serverless ETL pipeline built on AWS. It implements a **Medallion Architecture** (Bronze, Silver, Gold layers) to process sales data. The data is transformed using AWS Lambda, cataloged via AWS Glue, queried with Amazon Athena, and finally visualized through a containerized instance of Apache Superset.

## Architecture & Tech Stack
* **Data Storage:** Amazon S3 (Data Lake)
* **Compute & ETL:** AWS Lambda (Python, PyArrow, openpyxl)
* **Data Cataloging:** AWS Glue
* **Query Engine:** Amazon Athena (Serverless SQL)
* **Visualization:** Apache Superset (Docker)
* **Security:** AWS IAM (Least Privilege Policies)

## Repository Structure

\`\`\`text
.
├── docker/             # Contains `requirements-local.txt` to inject PyAthena dependencies into the Superset container.
├── IAM Policy/         # Contains `iam_superset_policy.json` demonstrating strict read/write access control.
├── images/             # Architecture diagrams and dashboard screenshots.
├── sql/                # SQL queries for dimensional modeling and Athena dataset creation (e.g., `modelo_gold.sql`).
├── src/                # Python scripts used for the AWS Lambda ETL processes.
├── .gitignore          # Rules to prevent the upload of sensitive credentials and environment variables.
└── README.md           # Project documentation.
\`\`\`

## Key Features & Implementations

1. **Automated Data Processing:** AWS Lambda functions automatically trigger and transform raw Excel/CSV data into optimized Parquet files, moving them sequentially through the Bronze, Silver, and Gold layers in S3.
2. **Semantic Layer Modeling:** The Gold layer is structured into a Star Schema (`fact_sales`, `dim_customer`, `dim_item`, `dim_date`, `dim_unit`).
3. **Advanced SQL Handling:** Handled Presto/Athena reserved keyword conflicts (e.g., aliasing `"group" AS item_group`) directly within the Superset SQL Lab to ensure seamless data visualization.
4. **Cloud Security:** Deployed custom IAM roles adhering strictly to the principle of least privilege, restricting Superset's access exclusively to the necessary S3 buckets and Athena workgroups.

## Dashboard Preview
*(Add your final Apache Superset dashboard screenshot here)*
![Superset Dashboard](images/dashboard_preview.png)

## Author
**Andrés Márquez**  
Data Engineer | Economics & Data Analytics