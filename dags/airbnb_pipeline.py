from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbnb_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # chạy thủ công trong UI, không tự schedule
    catchup=False,
    tags=["airbnb", "dbt"],
) as dag:

    spark_etl = DockerOperator(
        task_id="spark_etl",
        image="airbnb_spark:latest",
        api_version="auto",
        auto_remove=True,
        command="spark-submit /opt/spark_jobs/etl.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        # Truyền AWS_* và S3_BUCKET từ env của container Airflow sang container Spark
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_REGION": os.environ.get("AWS_REGION"),
            "S3_BUCKET": os.environ.get("S3_BUCKET"),
        },
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="airbnb_dbt:latest",
        api_version="auto",
        auto_remove=True,
        command="dbt run",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        # Truyền DBT_PROFILES_DIR và các biến REDSHIFT_* từ env của Airflow sang container dbt
        environment={
            "DBT_PROFILES_DIR": "/opt/dbt",
            "REDSHIFT_HOST": os.environ.get("REDSHIFT_HOST"),
            "REDSHIFT_PORT": os.environ.get("REDSHIFT_PORT", "5439"),
            "REDSHIFT_USER": os.environ.get("REDSHIFT_USER"),
            "REDSHIFT_PASSWORD": os.environ.get("REDSHIFT_PASSWORD"),
            "REDSHIFT_DBNAME": os.environ.get("REDSHIFT_DBNAME", "dev"),
            "REDSHIFT_SCHEMA": os.environ.get("REDSHIFT_SCHEMA", "analytics"),
        },
        mounts=[
            Mount(
                source="/host_mnt/d/Project/airbnb-data-pipeline/airbnb_dbt",
                target="/opt/dbt",
                type="bind",
            )
        ],
    )

    spark_etl >> dbt_run
