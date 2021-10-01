# Purpose: Batch read and display sales totals from Kafka in Avro format.
# Author:  Gary A. Stafford
# Date: 2021-09-28

import os

import boto3
import pyspark.sql.functions as F
import requests
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.window import Window

source_topic = "pagila.sales.summary.avro"
params = {}

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    global params
    params = get_parameters()

    df_sales = read_from_kafka()

    df_sales.show(100, truncate=False)


def read_from_kafka():
    spark = SparkSession \
        .builder \
        .appName("kafka-streaming-sales") \
        .getOrCreate()

    sales_summary_key = get_schema("pagila.sales.summary.avro-key")
    sales_summary_value = get_schema("pagila.sales.summary.avro-value")

    options_read = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "subscribe":
            source_topic,
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

    window = Window.partitionBy("region", "window_start").orderBy(F.col("timestamp").desc())

    df_sales = spark.read \
        .format("kafka") \
        .options(**options_read) \
        .load() \
        .select("timestamp",
                from_avro("key", sales_summary_key).alias("key"),
                from_avro("value", sales_summary_value).alias("data")) \
        .select("timestamp", "key", "data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .where(F.col("row") == 1).drop("row") \
        .select(F.col("region").alias("sales_region"),
                F.format_number("sales", 2).alias("sales"),
                F.format_number("orders", 0).alias("orders"),
                F.from_unixtime("window_start", format="yyyy-MM-dd HH:mm").alias("window_start"),
                F.from_unixtime("window_end", format="yyyy-MM-dd HH:mm").alias("window_end")) \
        .orderBy(F.col("window_start").desc(), F.regexp_replace("sales", ",", "").cast("float").desc())

    return df_sales


# ***** utility methods *****

def get_schema(artifact_id):
    """Get Avro schema from Apicurio Registry"""

    response = requests.get(
        f"{params['schema_registry_url']}/apis/registry/v2/groups/default/artifacts/{artifact_id}")
    json_format_schema = response.content.decode("utf-8")

    return json_format_schema


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    parameters = {
        "kafka_servers": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_servers")["Parameter"]["Value"],
        "kafka_demo_bucket": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_demo_bucket")["Parameter"]["Value"],
        "schema_registry_url": ssm_client.get_parameter(
            Name="/kafka_spark_demo/schema_registry_url_int")["Parameter"]["Value"],
    }

    return parameters


if __name__ == "__main__":
    main()
