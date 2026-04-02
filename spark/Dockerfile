FROM eclipse-temurin:11-jdk

RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Sử dụng version Spark/pyspark ổn định
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Tải Spark từ archive chính thức (tránh redirect HTML)
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar xz -C /opt

RUN mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Thêm thư viện Hadoop AWS và AWS SDK để hỗ trợ s3a://
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar

# Cài pyspark + python-dotenv để đọc env từ file .env trong etl.py
# Bỏ qua hạn chế PEP 668 trong môi trường container
RUN PIP_BREAK_SYSTEM_PACKAGES=1 pip3 install pyspark==3.5.1 python-dotenv

WORKDIR /opt/spark

# Copy ETL entrypoint into image so DockerOperator can run it
COPY etl.py /opt/spark_jobs/etl.py