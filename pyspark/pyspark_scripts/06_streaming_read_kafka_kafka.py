# Purpose: Streaming read from Kafka topic and aggregate
#          sales and orders by country to Kafka every minute
# Author:  Gary A. Stafford
# Date: 2021-09-08

import os

import boto3
import pyspark.sql.functions as F
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType

topic_input = "pagila.sales.spark.streaming.55"
topic_output = "pagila.sales.spark.streaming.out.55"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-streaming-sales-kafka") \
        .getOrCreate()

    df_sales = read_from_kafka(spark, params)

    summarize_sales(params, df_sales)


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


def summarize_sales(params, df_sales):
    options_write = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "topic":
            topic_output,
        "kafka.ssl.truststore.location":
            "/tmp/kafka.client.truststore.jks",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    }

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
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .groupBy("country") \
        .agg(F.count("amount"), F.sum("amount")) \
        .orderBy(F.col("sum(amount)").desc()) \
        .select(F.sha1("country").alias("id"),
                "country",
                (F.format_number(F.col("sum(amount)"), 2)).alias("sales"),
                (F.col("count(amount)")).alias("orders")) \
        .coalesce(1) \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .trigger(processingTime="1 minute") \
        .queryName("streaming_to_kafka") \
        .outputMode("complete") \
        .format("kafka") \
        .options(**options_write) \
        .option("checkpointLocation", "/checkpoint/kafka/") \
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
