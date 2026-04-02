# Airbnb Data Pipeline

This project implements an end-to-end batch data pipeline for Airbnb listings data.

The goal of the project is to demonstrate a modern **data engineering** stack on AWS, including:

- **Apache Airflow** for workflow orchestration and scheduling
- **Apache Spark (PySpark)** for scalable ETL from an S3 data lake (bronze → silver)
- **dbt + Amazon Redshift** for ELT, SQL-based data modeling, and dimensional modeling (staging → marts, fact/dimension tables)
- **Docker** for containerized, reproducible development environments

## Architecture Overview

- Airflow (webserver, scheduler, worker, triggerer, flower, Postgres, Redis) runs in Docker containers.
- A Spark image (`airbnb_spark`) runs an ETL job that reads `listings.csv` from S3 and writes cleaned parquet files back to S3.
- A dbt image (`airbnb_dbt`) runs `dbt run` to load data from S3/Redshift staging into mart tables in Redshift.
- The Airflow DAG `airbnb_pipeline` has two tasks:
  1. `spark_etl`: runs `spark-submit /opt/spark_jobs/etl.py` in the `airbnb_spark` container.
  2. `dbt_run`: runs `dbt run` in the `airbnb_dbt` container.

Execution flow: `spark_etl` → `dbt_run`.

## What I Built

- A containerized **Airflow** for production-style orchestration.
- A custom **Spark (PySpark)** ETL job that reads raw Airbnb listings from an S3 **data lake**, applies data cleaning and transformations, and writes partitioned parquet data back to S3 (bronze → silver).
- A **dbt** project on **Amazon Redshift** that implements ELT and **dimensional modeling** (staging models, dimension tables, and fact tables) on top of the cleaned data.
- An Airflow **DAG** that orchestrates the entire batch pipeline: Spark ETL → dbt models, using `DockerOperator` to run each step in its own container.
- Environment-driven configuration with a `.env` file so **secrets management** and deployment to different environments are easier (no hard-coded AWS/Redshift credentials).

## Data Engineering Keywords / Highlights

- Batch data pipeline, orchestration, workflow scheduling
- Data lake on S3 (bronze/silver layers), parquet storage
- PySpark ETL, data cleaning, transformation
- ELT with dbt, SQL-based transformations
- Dimensional modeling, star schema, fact & dimension tables
- Containerization with Docker
- Environment-based configuration, secrets management

## Project Structure

- `docker-compose.yaml`: defines the Docker stack (Airflow, Postgres, Redis, Spark/dbt images).
- `dags/airbnb_pipeline.py`: Airflow DAG orchestrating the Spark ETL and dbt runs.
- `spark/`:
  - `Dockerfile`: builds the `airbnb_spark` image (Spark + Hadoop AWS + pyspark + python-dotenv).
  - `etl.py`: Spark job that reads data from S3 (bronze), cleans it, and writes parquet to S3 (silver).
- `airbnb_dbt/`:
  - `dbt_project.yml`: dbt project configuration.
  - `profiles.yml`: dbt profile for Redshift, using `REDSHIFT_*` environment variables.
  - `models/`: staging and mart models (dim/fact).
-- `data/listings.csv`: raw data file used as the input dataset (bronze layer before uploading to S3).
-- `requirements.txt`: Python dependencies for Airflow (dbt, Docker provider, boto3, dotenv, ...).

## `airbnb_pipeline` DAG Details

- File: `dags/airbnb_pipeline.py`.
- Uses `DockerOperator` (from `apache-airflow-providers-docker`) to:
  - Start a `airbnb_spark:latest` container and run the Spark job.
  - Start a `airbnb_dbt:latest` container and run `dbt run` with a volume mount of the dbt project directory into `/opt/dbt`.
- Environment variables are read from the Airflow container using `os.environ` and passed into the Spark/dbt containers.

If you clone the project to a different path or use Linux/macOS, you only need to adapt the host directory that is mounted into the dbt container (see the `Mount` configuration in `airbnb_pipeline.py`).

## Technologies

- Apache Airflow 2.4.x (CeleryExecutor)
- Apache Spark 3.5.x + Hadoop AWS (S3A)
- dbt-core + dbt-redshift
- Amazon S3, Amazon Redshift (cluster or serverless)
- Docker, Docker Compose

---