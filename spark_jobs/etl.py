from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import coalesce, col, count, from_json, greatest, least, regexp_extract, to_date, regexp_replace, when
from pyspark.sql.types import *
from pyspark.sql.window import Window

load_dotenv()

# ENV
bucket = os.getenv("S3_BUCKET")
key = os.getenv("AWS_ACCESS_KEY_ID")
secret = os.getenv("AWS_SECRET_ACCESS_KEY")

BRONZE_PATH = f"s3a://{bucket}/raw/listings.csv"
SILVER_PATH = f"s3a://{bucket}/processed/listings/"
# GOLD_PATH = f"s3a://{bucket}/analytics/listings/"


listing_schema = StructType([
    StructField("id", LongType(), False),
    StructField("listing_url", StringType(), True),
    StructField("scrape_id", LongType(), True),
    StructField("last_scraped", StringType(), True),
    StructField("source", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("neighborhood_overview", StringType(), True),
    StructField("picture_url", StringType(), True),
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
    StructField("neighbourhood", StringType(), True),
    StructField("neighbourhood_cleansed", StringType(), True),
    StructField("neighbourhood_group_cleansed", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("property_type", StringType(), True),
    StructField("room_type", StringType(), True),
    StructField("accommodates", IntegerType(), True),
    StructField("bathrooms", IntegerType(), True),
    StructField("bathrooms_text", StringType(), True),
    StructField("bedrooms", IntegerType(), True),
    StructField("beds", IntegerType(), True),
    StructField("amenities", StringType(), True),
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
    StructField("has_availability", BooleanType(), True),
    StructField("availability_30", IntegerType(), True),
    StructField("availability_60", IntegerType(), True),
    StructField("availability_90", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
    StructField("calendar_last_scraped", StringType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("number_of_reviews_ltm", IntegerType(), True),
    StructField("number_of_reviews_l30d", IntegerType(), True),
    StructField("availability_eoy", IntegerType(), True),
    StructField("number_of_reviews_ly", IntegerType(), True),
    StructField("estimated_occupancy_l365d", IntegerType(), True),
    StructField("estimated_revenue_l365d", IntegerType(), True),
    StructField("first_review", StringType(), True),
    StructField("last_review", StringType(), True),
    StructField("review_scores_rating", DoubleType(), True),
    StructField("review_scores_accuracy", DoubleType(), True),
    StructField("review_scores_cleanliness", DoubleType(), True),
    StructField("review_scores_checkin", DoubleType(), True),
    StructField("review_scores_communication", DoubleType(), True),
    StructField("review_scores_location", DoubleType(), True),
    StructField("review_scores_value", DoubleType(), True),
    StructField("license", StringType(), True),
    StructField("instant_bookable", BooleanType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("calculated_host_listings_count_entire_homes", IntegerType(), True),
    StructField("calculated_host_listings_count_private_rooms", IntegerType(), True),
    StructField("calculated_host_listings_count_shared_rooms", IntegerType(), True),
    StructField("reviews_per_month", DoubleType(), True)
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

def read_data_S3(spark, path):
    return spark.read.csv(
        path,
        header=True,
        inferSchema=True,
        quote='"',
        escape='"',
        multiLine=True
    )

def apply_schema(df, schema):
    for field in schema.fields:
        col_name = field.name
        col_type = field.dataType

        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))

    return df

def clean_data(df):
    cast_dict = {
        "last_scraped",
        "host_since",
        "first_review",
        "last_review",
        "calendar_updated",
        "calendar_last_scraped"
    }

    # Change data types and clean text fields
    for col_name in cast_dict:
        df = df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))

    df = df.withColumn("price", regexp_replace(col("price"), "[$,]", "").cast(DecimalType(10, 2)))
    df = df.withColumn("host_response_rate", regexp_replace(col("host_response_rate"), "[%]", "").cast(DecimalType(10, 2)))
    df = df.withColumn("host_acceptance_rate", regexp_replace(col("host_acceptance_rate"), "[%]", "").cast(DecimalType(10, 2)))
    df = df.withColumn("description", regexp_replace(col("description"), "[\r\n]|<br\\s*/?>", " "))

    # Fix anomaly
    df = df.withColumn(
        "min_tmp",
        least(col("minimum_nights"), col("maximum_nights"))
    ).withColumn(
        "max_tmp",
        greatest(col("minimum_nights"), col("maximum_nights"))
    ).drop(
        "minimum_nights", "maximum_nights"
    ).withColumnRenamed(
        "min_tmp", "minimum_nights"
    ).withColumnRenamed(
        "max_tmp", "maximum_nights"
    )
    
    # Resolve missing values
    df = df.withColumn(
        "bathrooms",
        when(
            col("bathrooms").isNull() & col("bathrooms_text").isNotNull(),
            regexp_extract(col("bathrooms_text"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType())
        ).otherwise(col("bathrooms"))
    )

    df = df.withColumn(
        "bathrooms_text",
        when(
            col("bathrooms_text").isNull() & col("bathrooms").isNotNull(),
            when(col("bathrooms") == 1, "one bath")
            .when(col("bathrooms") == 2, "two baths")
            .otherwise(col("bathrooms").cast("string"))
        ).otherwise(col("bathrooms_text"))
    )

    df = df.withColumn(
        "bedrooms",
        coalesce(col("bedrooms"), col("accommodates"))
    ).withColumn(
        "beds",
        coalesce(col("beds"), col("accommodates"))
    )

    df = df.withColumn(
        "amenities_clean",
        regexp_replace("amenities", '""', '"')
    )

    df = df.withColumn(
        "amenities_array",
        from_json(col("amenities_clean"), ArrayType(StringType()))
    )

    df = df.fillna({
        "name": "No name",
        "description": "No description",
        "bathrooms": 1.0,
        "bathrooms_text": "one bath",
        "host_response_time": "a few days or more",
        "host_response_rate": 0.0,
        "host_acceptance_rate": 0.0,
        "host_is_superhost": False,
        "has_availability": True,
        "estimated_occupancy_l365d": 0,
        "price": 0.0,
    })

    w = Window.partitionBy("host_id")
    df = df.withColumn(
        "host_listings_count",
        when(
            col("host_listings_count").isNull(),
            count("*").over(w)
        ).otherwise(col("host_listings_count"))
    )

    df = df.drop(
        "host_name",
        "host_location",
        "host_about",
        "host_location",
        "host_thumbnail_url",
        "host_picture_url",
        "host_neighbourhood",
        "host_verifications",
        "host_total_listings_count",
        "host_has_profile_pic",
        "host_identity_verified",
        "license",
        "neighborhood_overview",
        "neighbourhood",
        "calendar_updated",
    )

    df = df.filter(col("id").isNotNull())
    df = df.filter(col("first_review").isNotNull() & col("last_review").isNotNull())
    df = df.filter(col("minimum_minimum_nights").isNotNull() & col("maximum_minimum_nights").isNotNull() & col("minimum_maximum_nights").isNotNull() & col("maximum_maximum_nights").isNotNull())

    return df

def main():
    spark = create_spark_session()
    df = read_data_S3(spark, BRONZE_PATH)
    df = apply_schema(df, listing_schema)
    df_cleaned = clean_data(df)

    # Write the cleaned data back to S3 in Parquet format
    df_cleaned.write \
        .mode("overwrite") \
        .partitionBy("neighbourhood_group_cleansed") \
        .parquet(SILVER_PATH)
    
    print("ETL DONE")
    spark.stop()


if __name__ == "__main__":
    main()