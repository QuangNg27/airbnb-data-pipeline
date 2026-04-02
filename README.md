# Airbnb Data Pipeline

This project implements an end-to-end data pipeline for Airbnb data using:

- **Apache Airflow** for orchestration
- **Apache Spark** for ETL from S3 (bronze → silver)
- **dbt + Amazon Redshift** for data modeling (staging → marts)
- **Docker Compose** to run the whole stack locally

## Architecture Overview

- Airflow (webserver, scheduler, worker, triggerer, flower, Postgres, Redis) runs in Docker containers.
- A Spark image (`airbnb_spark`) runs an ETL job that reads `listings.csv` from S3 and writes cleaned parquet files back to S3.
- A dbt image (`airbnb_dbt`) runs `dbt run` to load data from S3/Redshift staging into mart tables in Redshift.
- The Airflow DAG `airbnb_pipeline` has two tasks:
  1. `spark_etl`: runs `spark-submit /opt/spark_jobs/etl.py` in the `airbnb_spark` container.
  2. `dbt_run`: runs `dbt run` in the `airbnb_dbt` container.

Execution flow: `spark_etl` → `dbt_run`.

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
- `data/listings.csv`: raw data file to upload to S3 (bronze layer).
- `requirements.txt`: Python dependencies for Airflow (dbt, Docker provider, boto3, dotenv, ...).

## Prerequisites

1. **Required tools**
   - Docker Desktop (with WSL2 backend on Windows).
   - Docker Compose v2.

2. **Create `.env` file in the project root** (same level as `docker-compose.yaml`):

   ```env
   AWS_ACCESS_KEY_ID=...
   AWS_SECRET_ACCESS_KEY=...
   AWS_REGION=ap-southeast-1

   S3_BUCKET=quang-data-pipeline

   REDSHIFT_HOST=...redshift(-serverless).amazonaws.com
   REDSHIFT_PORT=5439
   REDSHIFT_DBNAME=dev
   REDSHIFT_USER=admin
   REDSHIFT_PASSWORD=...
   REDSHIFT_SCHEMA=analytics

   AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
   AIRFLOW_UID=50000
   ```

   Notes:
   - `REDSHIFT_HOST` must be the full endpoint from AWS (cluster or serverless workgroup).
   - In real environments, restrict Redshift access via security groups/VPC instead of `0.0.0.0/0`.

3. **Upload raw data to S3**

   - Create an S3 bucket matching `S3_BUCKET` above.
   - Upload `data/listings.csv` to:

     ```
     s3://<S3_BUCKET>/raw/listings.csv
     ```

## Running the Stack

1. **Build images**

   ```bash
   docker compose build
   ```

2. **Start the Airflow stack**

   ```bash
   docker compose up -d
   ```

3. **Access the Airflow UI**

   - Open: http://localhost:8080
   - Default credentials:
     - username: `airflow`
     - password: `airflow`

4. **Trigger the DAG**

   - In the Airflow UI, enable the DAG `airbnb_pipeline`.
   - Click "Trigger DAG" to run it.
   - In Graph view:
     - Check the `spark_etl` task logs to verify the S3 ETL.
     - Then check the `dbt_run` task logs to verify dbt execution on Redshift.

## `airbnb_pipeline` DAG Details

- File: `dags/airbnb_pipeline.py`.
- Uses `DockerOperator` (from `apache-airflow-providers-docker`) to:
  - Start a `airbnb_spark:latest` container and run the Spark job.
  - Start a `airbnb_dbt:latest` container and run `dbt run` with a volume mount:
    - Host: `D:\Project\airbnb-data-pipeline\airbnb_dbt`
    - Container: `/opt/dbt`
- Environment variables are read from the Airflow container using `os.environ` and passed into the Spark/dbt containers.

## Operational Notes

- Changing `.env` does **not** require rebuilding images, but you must recreate or restart containers so they pick up new values:

  ```bash
  docker compose down
  docker compose up -d
  ```

- If you only change the DAG or dbt models, `docker compose restart airflow-webserver airflow-scheduler` is usually enough.
- To debug Redshift connectivity:
  - Check `REDSHIFT_HOST` inside the Airflow container: `docker compose exec airflow-webserver env | grep REDSHIFT_HOST`.
  - Ensure the Redshift endpoint is reachable and its security group allows your IP (or Docker host) to connect.

## Technologies

- Apache Airflow 2.4.x (CeleryExecutor)
- Apache Spark 3.5.x + Hadoop AWS (S3A)
- dbt-core + dbt-redshift
- Amazon S3, Amazon Redshift (cluster or serverless)
- Docker, Docker Compose

---