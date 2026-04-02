FROM python:3.11-slim

WORKDIR /opt/dbt

# Install system deps (git thường được dbt dùng cho packages, metadata)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install dbt and adapters
RUN pip install --no-cache-dir dbt-core dbt-redshift

# Project dbt sẽ được mount vào /opt/dbt qua docker-compose, không COPY từ ngoài build context
ENV DBT_PROFILES_DIR=/opt/dbt

CMD ["dbt", "run"]
