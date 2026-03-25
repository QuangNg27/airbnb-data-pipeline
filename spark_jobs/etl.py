from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col

load_dotenv()

# ENV
bucket = os.getenv("S3_BUCKET")
key = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")

# Spark session
spark = SparkSession.builder \
    .appName("ETL Step 1") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.access.key", key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# Read data from S3
df = spark.read.csv(
    f"s3a://{bucket}/raw/listings.csv",
    header=True,
    inferSchema=True
)

# Clean data
df = df.dropna(subset=["id", "price"])

# Transform data
df = df.withColumn("price", col("price").cast("double"))

# Write processed data back to S3 in Parquet format
df.write.mode("overwrite").parquet(
    f"s3a://{bucket}/processed/listings/"
)

print("ETL DONE")