# Purpose: Streaming read from Kafka topic, join with static data,
#          and aggregate in windows by sales region to the console every minute
#          Show 24 = 8 regions x 3 windows
# Author:  Gary A. Stafford
# Date: 2021-09-08

import os

import boto3
import pyspark.sql.functions as F
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType

topic_input = "pagila.sales.spark.streaming.region"
regions_data = "sales_regions.csv"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-streaming-sales-join") \
        .getOrCreate()

    df_regions = read_from_csv(spark, params)
    df_regions.cache()

    df_sales = read_from_kafka(spark, params)
    summarize_sales(df_sales, df_regions)


def read_from_kafka(spark, params):
    options_read = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "subscribe":
            topic_input,
        "startingOffsets":
            "earliest",
        "kafka.ssl.truststore.location":
            "/tmp/kafka.client.truststore.jks",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

    df_sales = spark.readStream \
        .format("kafka") \
        .options(**options_read) \
        .load()

    return df_sales


def read_from_csv(spark, params):
    schema = StructType([
        StructField("country", StringType(), False),
        StructField("region", StringType(), False)
    ])

    df_sales = spark.read \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{regions_data}",
             schema=schema, header=True, sep=",")

    return df_sales


def summarize_sales(df_sales, df_regions):
    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", TimestampType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    ds_sales = df_sales \
        .selectExpr("CAST(value AS STRING)", "timestamp") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .join(df_regions, on=["country"], how="leftOuter") \
        .na.fill("Unassigned") \
        .groupBy("region") \
        .agg(F.count("amount"), F.sum("amount")) \
        .select(F.col("region").alias("sales_region"),
                F.format_number("sum(amount)", 2).alias("sales"),
                F.format_number("count(amount)", 0).alias("orders")) \
        .orderBy(F.col("sum(amount)").desc()) \
        .coalesce(1) \
        .writeStream \
        .queryName("streaming_regional_sales") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 24) \
        .option("truncate", False) \
        .start()

    ds_sales.awaitTermination()


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        "kafka_servers": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_servers")["Parameter"]["Value"],
        "kafka_demo_bucket": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_demo_bucket")["Parameter"]["Value"],
    }

    return params


if __name__ == "__main__":
    main()
