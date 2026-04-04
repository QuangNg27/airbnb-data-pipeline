# Airbnb Data Pipeline (Student Project)

## Overview

This project builds an end-to-end batch data pipeline for Airbnb listings data. It simulates how raw data is ingested, processed, and transformed into analytics-ready datasets using modern data engineering tools.

---

## Architecture Overview

The pipeline follows a **medallion architecture (bronze → silver → gold)** to organize data processing layers.

The pipeline uses the following components:

* Apache Airflow for orchestration
* Apache Spark (PySpark) for ETL processing
* Amazon S3 as the data lake (bronze → silver)
* dbt + Amazon Redshift for ELT and data modeling (staging → marts)
* Docker for containerized execution

---

## Data Flow

(Medallion layers: Bronze → Silver → Gold)

```text
S3 (raw CSV - bronze)
    ↓
Spark ETL (clean, transform)
    ↓
S3 (parquet - silver)
    ↓
Redshift (staging tables)
    ↓
dbt (dim + fact tables)
    ↓
Analytics / BI
```

---

## How It Works

### 1. Data Ingestion (Bronze Layer)

* Raw Airbnb listings data is stored as CSV files in Amazon S3
* This layer represents the raw data lake

### 2. Data Processing (Spark ETL)

* A PySpark job reads data from S3
* Performs data cleaning:

  * Handles missing values
  * Fixes inconsistent data types
  * Removes invalid records
* Writes processed data as parquet files back to S3 (silver layer)

### 3. Data Transformation (dbt + Redshift)

* Cleaned data is loaded into Redshift staging tables
* dbt is used to:

  * Standardize data in staging models
  * Build fact and dimension tables
* A star schema is applied for analytics use cases

### 4. Orchestration (Airflow)

* Airflow DAG `airbnb_pipeline` manages the workflow
* Two main tasks:

  * `spark_etl`: runs Spark job
  * `dbt_run`: runs dbt models
* Execution order:

spark_etl → dbt_run

---

## What I Built

* A Spark ETL pipeline to process raw data from S3
* A dbt project to build analytical data models in Redshift
* An Airflow DAG to orchestrate the entire workflow
* A Docker-based environment for reproducibility

---

## Data Modeling

The warehouse follows a star schema design:

* Fact table:

  * `fact_listings`

* Dimension tables:

  * `dim_host`
  * `dim_location`
  * `dim_property_type`

---

## Project Structure

* `docker-compose.yaml`: defines services (Airflow, Spark, dbt, Postgres, Redis)
* `dags/`: Airflow DAG
* `spark/`: Spark ETL job
* `airbnb_dbt/`: dbt project
* `data/`: raw dataset

---

## How to Run

```bash
git clone <repo-url>
cd <repo>
docker-compose up -d
```

Access Airflow at:
[http://localhost:8080](http://localhost:8080)

Trigger DAG: `airbnb_pipeline`

---

## Notes

This project is built for learning purposes but follows the structure of a real-world data engineering pipeline.

---
