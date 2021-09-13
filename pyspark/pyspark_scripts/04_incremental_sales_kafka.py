# Purpose: Batch write incremental sales data from S3 to a new Kafka topic
#          Use a delay between each message to simulate real-time streaming data
# Author:  Gary A. Stafford
# Date: 2021-09-04

import os
import time

import boto3
import pyspark.sql.functions as F
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

sales_data = "sales_incremental_large.csv"
topic_output = "pagila.sales.spark.streaming"
time_between_messages = 0.5  # 1800 messages * .5 seconds = ~15 minutes

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-incremental-sales") \
        .getOrCreate()

    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", StringType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    df_sales = read_from_csv(spark, params, schema)
    df_sales.cache()

    write_to_kafka(spark, params, df_sales, schema)


def read_from_csv(spark, params, schema):
    df_sales = spark.read \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{sales_data}",
             schema=schema, header=True, sep="|")

    return df_sales


def write_to_kafka(spark, params, df_sales, schema):
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

    sales_count = df_sales.count()

    for r in range(0, sales_count):
        row = df_sales.collect()[r]
        df_message = spark.createDataFrame([row], schema)

        df_message = df_message \
            .drop("payment_date") \
            .withColumn("payment_date", F.current_timestamp())

        df_message \
            .selectExpr("CAST(payment_id AS STRING) AS key",
                        "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()

        df_message.show(1)

        time.sleep(time_between_messages)


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
