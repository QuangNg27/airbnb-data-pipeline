from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col
from pyspark.sql.types import *

load_dotenv()

# ENV
bucket = os.getenv("S3_BUCKET")
key = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")

BRONZE_PATH = f"s3a://{bucket}/raw/listings.csv"
SILVER_PATH = f"s3a://{bucket}/processed/listings/"
GOLD_PATH = f"s3a://{bucket}/analytics/listings/"


listing_schema = StructType([
    StructField("id", LongType(), False),
    StructField("listing_url", StringType(), True),
    StructField("scrape_id", LongType(), True),
    StructField("last_scraped", StringType(), True),

    StructField("source", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("picture_url", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("property_type", StringType(), True),
    StructField("room_type", StringType(), True),
    StructField("accommodates", IntegerType(), True),

    StructField("bathrooms", DoubleType(), True),
    StructField("bathrooms_text", StringType(), True),
    StructField("bedrooms", DoubleType(), True),
    StructField("beds", DoubleType(), True),

    StructField("amenities", StringType(), True),

    StructField("host_id", LongType(), True),
    StructField("host_url", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("host_since", StringType(), True),

    StructField("host_location", StringType(), True),
    StructField("host_about", StringType(), True),

    StructField("host_response_time", StringType(), True),
    StructField("host_response_rate", StringType(), True),
    StructField("host_acceptance_rate", StringType(), True),

    StructField("host_is_superhost", BooleanType(), True),
    StructField("host_thumbnail_url", StringType(), True),
    StructField("host_picture_url", StringType(), True),

    StructField("host_neighbourhood", StringType(), True),
    StructField("host_listings_count", IntegerType(), True),
    StructField("host_total_listings_count", IntegerType(), True),

    StructField("host_verifications", StringType(), True),
    StructField("host_has_profile_pic", BooleanType(), True),
    StructField("host_identity_verified", BooleanType(), True),

    StructField("has_availability", BooleanType(), True),

    StructField("availability_30", IntegerType(), True),
    StructField("availability_60", IntegerType(), True),
    StructField("availability_90", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
    StructField("availability_eoy", IntegerType(), True),

    StructField("estimated_occupancy_l365d", DoubleType(), True),
    StructField("estimated_revenue_l365d", DoubleType(), True),

    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("calculated_host_listings_count_entire_homes", IntegerType(), True),
    StructField("calculated_host_listings_count_private_rooms", IntegerType(), True),
    StructField("calculated_host_listings_count_shared_rooms", IntegerType(), True),

    StructField("license", StringType(), True),
    StructField("instant_bookable", BooleanType(), True),

    StructField("neighborhood_overview", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("neighbourhood_cleansed", StringType(), True),
    StructField("neighbourhood_group_cleansed", StringType(), True),

    StructField("first_review", StringType(), True),
    StructField("last_review", StringType(), True),

    StructField("review_scores_rating", DoubleType(), True),
    StructField("review_scores_accuracy", DoubleType(), True),
    StructField("review_scores_cleanliness", DoubleType(), True),
    StructField("review_scores_checkin", DoubleType(), True),
    StructField("review_scores_communication", DoubleType(), True),
    StructField("review_scores_location", DoubleType(), True),
    StructField("review_scores_value", DoubleType(), True),

    StructField("reviews_per_month", DoubleType(), True),

    StructField("number_of_reviews", IntegerType(), True),
    StructField("number_of_reviews_ltm", IntegerType(), True),
    StructField("number_of_reviews_l30d", IntegerType(), True),
    StructField("number_of_reviews_ly", IntegerType(), True),

    StructField("price", StringType(), True),

    StructField("minimum_nights", IntegerType(), True),
    StructField("maximum_nights", IntegerType(), True),

    StructField("minimum_minimum_nights", IntegerType(), True),
    StructField("maximum_minimum_nights", IntegerType(), True),

    StructField("minimum_maximum_nights", IntegerType(), True),
    StructField("maximum_maximum_nights", IntegerType(), True),

    StructField("minimum_nights_avg_ntm", DoubleType(), True),
    StructField("maximum_nights_avg_ntm", DoubleType(), True),

    StructField("calendar_updated", StringType(), True),
    StructField("calendar_last_scraped", StringType(), True)
])


# Spark session
def create_spark_session():
    return ( 
        SparkSession.builder \
        .appName("ETL listings") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    )

def read_data(spark, path):
    return spark.read.csv(
        path,
        header=True,
        schema=listing_schema
    )

def clean_data(df):
    df = df.dropna(subset=["id", "price"])
    df = df.withColumn("price", col("price").cast("double"))
    return df

def main():
    spark = create_spark_session()
    df = read_data(spark, BRONZE_PATH)
    df_cleaned = clean_data(df)
    df_cleaned.write.mode("overwrite").parquet(SILVER_PATH)


if __name__ == "__main__":
    main()
    print("ETL DONE")