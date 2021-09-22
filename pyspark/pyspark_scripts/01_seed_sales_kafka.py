# Purpose: Batch write initial sales data from S3 to a new Kafka topic
# Author:  Gary A. Stafford
# Date: 2021-09-22

import os

import boto3
import pyspark.sql.functions as F
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType
from pyspark.sql.window import Window

sales_data = "sales_seed.csv"
topic_output = "pagila.sales.spark.streaming"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-seed-sales") \
        .getOrCreate()

    df_sales = read_from_csv(spark, params)

    write_to_kafka(params, df_sales)


def read_from_csv(spark, params):
    schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", FloatType(), False),
        StructField("payment_date", StringType(), False),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("country", StringType(), False),
    ])

    df_sales = spark.read \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{sales_data}",
             schema=schema, header=True, sep="|")

    # optional
    df_sales = update_payment_date(df_sales)

    return df_sales


def write_to_kafka(params, df_sales):
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

    df_sales \
        .selectExpr("CAST(payment_id AS STRING) AS key",
                    "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .options(**options_write) \
        .save()


def update_payment_date(df):
    """Update existing payment date to a current timestamp for streaming simulation"""

    record_count = 250
    window = Window.orderBy("payment_id")
    df = df \
        .drop("payment_date") \
        .withColumn("index", F.row_number().over(window)) \
        .withColumn("payment_date",
                    (F.unix_timestamp(F.current_timestamp()) -
                     (record_count - F.col("index"))).cast(IntegerType())) \
        .drop("index")

    return df


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
