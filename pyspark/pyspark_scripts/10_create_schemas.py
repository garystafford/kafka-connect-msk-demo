# Purpose: Create (6) schemas in Apicurio Registry.
# Author:  Gary A. Stafford
# Date: 2021-09-28

import json
import os

import boto3
import requests

params = {}

os.environ['AWS_DEFAULT_REGION'] = "us-east-1"
ssm_client = boto3.client("ssm")


def main():
    global params
    params = get_parameters()

    artifact_id = "pagila.sales.csv"
    data = '''{"name":"Sales","type":"record",
        "doc":"Schema for CSV-format sales data",
        "fields":[
        {"name":"payment_id","type":"int"},
        {"name":"customer_id","type":"int"},
        {"name":"amount","type":"float"},
        {"name":"payment_date","type":"string"},
        {"name":"city","type":["string","null"]},
        {"name":"district","type":["string","null"]},
        {"name":"country","type":"string"}]}'''
    create_schema(artifact_id, data)

    artifact_id = "pagila.sales.regions.csv"
    data = '''{"name":"Regions","type":"record",
        "doc":"Schema for CSV-format sales regions data",
        "fields":[
        {"name":"country","type":"string"},
        {"name":"region","type":"string"}]}'''
    create_schema(artifact_id, data)

    artifact_id = "pagila.sales.avro-key"
    data = '''{"name":"Key","type":"int",
        "doc":"Schema for pagila.sales.avro Kafka topic key"}'''
    create_schema(artifact_id, data)

    artifact_id = "pagila.sales.avro-value"
    data = '''{"name":"Value","type":"record",
        "doc":"Schema for pagila.sales.avro Kafka topic value",
        "fields":[
        {"name":"payment_id","type":"int"},
        {"name":"customer_id","type":"int"},
        {"name":"amount","type":"float"},
        {"name":"payment_date","type":"long","logicalType":"timestamp-millis"},
        {"name":"city","type":["string","null"]},
        {"name":"district","type":["string","null"]},
        {"name":"country","type":"string"}]}'''
    create_schema(artifact_id, data)

    artifact_id = "pagila.sales.summary.avro-key"
    data = '''{"name":"Key","type":"int",
        "doc":"Schema for pagila.sales.summary.avro Kafka topic key"}'''
    create_schema(artifact_id, data)

    artifact_id = "pagila.sales.summary.avro-value"
    data = '''{"name":"Value","type":"record",
        "doc":"Schema for pagila.sales.summary.avro Kafka topic value",
        "fields":[
        {"name":"region","type":"string"},
        {"name":"sales","type":"float"},
        {"name":"orders","type":"int"},
        {"name":"window_start","type":"long","logicalType":"timestamp-millis"},
        {"name":"window_end","type":"long","logicalType":"timestamp-millis"}]}'''
    create_schema(artifact_id, data)


def create_schema(artifact_id, data):
    """Delete existing schema, create new schema, and retrieve schema"""

    delete_schema(artifact_id)
    print(json.dumps(json.loads(post_schema(artifact_id, data)), indent=4))
    print(json.dumps(json.loads(get_schema(artifact_id)), indent=4))


def post_schema(artifact_id, data):
    """Post AVRO schema to Apicurio Registry"""

    response = requests.post(
        url=f"{params['schema_registry_url']}/apis/registry/v2/groups/default/artifacts",
        data=data,
        headers={"X-Registry-ArtifactId": artifact_id})

    json_format_schema = response.content.decode("utf-8")

    return json_format_schema


def get_schema(artifact_id):
    """Get AVRO schema from Apicurio Registry"""

    response = requests.get(
        f"{params['schema_registry_url']}/apis/registry/v2/groups/default/artifacts/{artifact_id}")

    json_format_schema = response.content.decode("utf-8")

    return json_format_schema


def delete_schema(artifact_id):
    """Delete AVRO schema from Apicurio Registry"""

    try:
        response = requests.delete(
            f"{params['schema_registry_url']}/apis/registry/v2/groups/default/artifacts/{artifact_id}")

        return response.content.decode("utf-8")
    except:
        return f"Schema not found: {artifact_id}"


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    parameters = {
        "schema_registry_url": ssm_client.get_parameter(
            Name="/kafka_spark_demo/schema_registry_url_int")["Parameter"]["Value"],
    }

    return parameters


if __name__ == "__main__":
    main()
