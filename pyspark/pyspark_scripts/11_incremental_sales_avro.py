# Purpose: Write sales data from CSV to a new Kafka topic in Avro format.
#          Use a delay between each message to simulate an event stream of sales data.
# Author:  Gary A. Stafford
# Date: 2021-09-28

import os
import time

import boto3
import pyspark.sql.functions as F
import requests
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import LongType

sink_topic = "pagila.sales.avro"
delay_between_messages = 0.333  # 1800 messages * .333 second delay = ~10 minutes added latency
params = {}

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    global params
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-incremental-sales") \
        .getOrCreate()

    json_format_schema = get_schema("pagila.sales.csv")
    schema = struct_from_json(spark, json_format_schema)
    df_sales = read_from_csv(spark, "sales_incremental_large.csv", schema, "|")
    df_sales.show(5, truncate=False)

    write_to_kafka(spark, df_sales)


def write_to_kafka(spark, df_sales):
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

    sales_schema_key = get_schema("pagila.sales.avro-key")
    sales_schema_value = get_schema("pagila.sales.avro-value")

    sales_count = df_sales.count()

    for r in range(0, sales_count):
        row = df_sales.collect()[r]
        df_message = spark.createDataFrame([row], df_sales.schema)

        df_message \
            .drop("payment_date") \
            .withColumn("payment_date",
                        F.unix_timestamp(F.current_timestamp()).cast(LongType())) \
            .select(to_avro("customer_id", sales_schema_key).alias("key"),
                    to_avro(F.struct("*"), sales_schema_value).alias("value")) \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()

        time.sleep(delay_between_messages)


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
