# Purpose: Streaming read from Kafka topic in Avro format. Enrich and aggregate
#          current sales by sales region to second Kafka topic every n minutes.
# Author:  Gary A. Stafford
# Date: 2021-09-28

import os

import boto3
import pyspark.sql.functions as F
import requests
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import IntegerType, FloatType, LongType

source_topic = "pagila.sales.avro"
sink_topic = "pagila.sales.summary.avro"
params = {}

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    global params
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-streaming-sales") \
        .getOrCreate()

    csv_sales_regions_schema = get_schema("pagila.sales.regions.csv")
    schema = struct_from_json(spark, csv_sales_regions_schema)
    df_regions = read_from_csv(spark, "sales_regions.csv", schema, ",")
    df_regions.cache()
    df_regions.show(5, truncate=False)

    df_sales = read_from_kafka(spark)

    summarize_sales(df_sales, df_regions)


def read_from_kafka(spark):
    sales_schema_value = get_schema("pagila.sales.avro-value")

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

    df_sales = spark.readStream \
        .format("kafka") \
        .options(**options_read) \
        .load() \
        .select(from_avro("value", sales_schema_value).alias("data"), "timestamp") \
        .select("data.*", "timestamp")

    return df_sales


def summarize_sales(df_sales, df_regions):
    sales_summary_key = get_schema("pagila.sales.summary.avro-key")
    sales_summary_value = get_schema("pagila.sales.summary.avro-value")

    options_write = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "topic":
            sink_topic,
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

    ds_sales = df_sales \
        .join(df_regions, on=["country"], how="leftOuter") \
        .na.fill("Unassigned") \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("region", F.window("timestamp", "10 minutes", "5 minutes")) \
        .agg(F.sum("amount"), F.count("amount")) \
        .orderBy(F.col("window").desc(), F.col("sum(amount)").desc()) \
        .select("region",
                F.col("sum(amount)").cast(FloatType()).alias("sales"),
                F.col("count(amount)").cast(IntegerType()).alias("orders"),
                F.unix_timestamp("window.start").cast(LongType()).alias("window_start"),
                F.unix_timestamp("window.end").cast(LongType()).alias("window_end")) \
        .coalesce(1) \
        .select(to_avro(F.col("window_start").cast(IntegerType()), sales_summary_key).alias("key"),
                to_avro(F.struct("*"), sales_summary_value).alias("value")) \
        .writeStream \
        .trigger(processingTime="2 minute") \
        .queryName("streaming_to_kafka") \
        .outputMode("complete") \
        .format("kafka") \
        .options(**options_write) \
        .option("checkpointLocation", "/checkpoint/kafka/") \
        .start()

    ds_sales.awaitTermination()


# ***** utility methods *****

def read_from_csv(spark, source_data, schema, sep):
    """Read CSV data from S3"""

    df = spark.read \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{source_data}",
             schema=schema, header=True, sep=sep)

    return df


def struct_from_json(spark, json_format_schema):
    """Returns a schema as a pyspark.sql.types.StructType from Avro schema"""

    df = spark \
        .read \
        .format("avro") \
        .option("avroSchema", json_format_schema) \
        .load()

    df.printSchema()

    return df.schema


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
