# Purpose: Batch read Kafka output topic and display,
#           top 25 total sales by country to console
# Author:  Gary A. Stafford
# Date: 2021-09-06

import os

import boto3
import pyspark.sql.functions as F
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, TimestampType
from pyspark.sql.window import Window

topic_input = "pagila.sales.spark.streaming.out"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-batch-sales") \
        .getOrCreate()

    df_sales = read_from_kafka(spark, params)

    summarize_sales(df_sales)


def read_from_kafka(spark, params):
    schema = StructType([
        StructField("country", StringType(), False),
        StructField("sales", StringType(), False),
        StructField("orders", IntegerType(), False),
        StructField("start", TimestampType(), False),
        StructField("end", TimestampType(), True),
    ])

    options_read = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "subscribe":
            topic_input,
        "startingOffsets":
            "earliest",
        "endingOffsets":
            "latest",
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

    window = Window.partitionBy("country").orderBy(F.col("timestamp").desc())

    df_sales = spark.read \
        .format("kafka") \
        .options(**options_read) \
        .load() \
        .selectExpr("CAST(value AS STRING)", "timestamp") \
        .select(F.from_json("value", schema=schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp") \
        .withColumn("row", F.row_number().over(window)) \
        .where(F.col("row") == 1).drop("row") \
        .select("country", "sales", "orders") \
        .coalesce(1) \
        .orderBy(F.regexp_replace("sales", ",", "").cast("float"), ascending=False)

    return df_sales


def summarize_sales(df_sales):
    df_sales \
        .write \
        .format("console") \
        .option("numRows", 25) \
        .option("truncate", False) \
        .save()


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
