# Airbnb Data Pipeline

## Overview

This is a personal data engineering project that builds an end-to-end batch data pipeline for Airbnb listings data.

The pipeline processes raw CSV data from a data lake, transforms it using Spark, and loads it into a data warehouse where dbt is used to create analytics-ready tables.

---

## Architecture Overview

In this project, I used the following tools:

* **Apache Airflow** to schedule and orchestrate the pipeline
* **Apache Spark (PySpark)** to process raw data
* **Amazon S3** as a data lake (store raw and processed data)
* **dbt + Amazon Redshift** to transform data into analytics tables
* **Docker** to run everything in a reproducible environment

---

## Data Flow

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

## What I Built

* **Spark ETL Job**

  * Reads raw CSV data from S3
  * Handles data cleaning (nulls, types, inconsistent values)
  * Writes optimized parquet files back to S3 (silver layer)

* **dbt Project (Redshift)**

  * Staging models to standardize raw data
  * Transformation models to build fact and dimension tables

* **Airflow DAG (`airbnb_pipeline`)**

  * Orchestrates the pipeline
  * Runs Spark ETL first, then dbt models
  * Uses `DockerOperator` to isolate execution environments

* **Docker Setup**

  * Airflow, Spark, and dbt run in separate containers
  * Ensures reproducibility across environments

---

## Data Modeling

I implemented a simple **star schema**:

* **Fact table:**

  * `fact_listings`

* **Dimension tables:**

  * `dim_host`
  * `dim_location`
  * `dim_property_type`

---

## Airflow DAG

The DAG `airbnb_pipeline` contains two main tasks:

1. **spark_etl**

   * Executes `spark-submit` inside the Spark container
   * Processes raw data from S3 and writes parquet output

2. **dbt_run**

   * Executes `dbt run` inside the dbt container
   * Builds staging and mart tables in Redshift

Execution order:

spark_etl → dbt_run

---

## Project Structure

* `docker-compose.yaml`: defines all services
* `dags/`: Airflow DAG
* `spark/`: Spark job
* `airbnb_dbt/`: dbt project
* `data/`: raw dataset

---

## How to Run

```bash
git clone <repo-url>
cd <repo>
docker-compose up -d
```

Then open Airflow at:

[http://localhost:8080](http://localhost:8080)

Trigger DAG: `airbnb_pipeline`

---

## Technologies Used

* Apache Airflow
* Apache Spark (PySpark)
* dbt
* Amazon S3
* Amazon Redshift
* Docker

---

## Notes

This project is built for learning purposes, so it simplifies some aspects of a real production system. However, it still follows the general structure of a modern data pipeline.

---
