#!/bin/bash

# Purpose: Create SSM parameters for demo
# Author:  Gary A. Stafford (2021-08-23)

aws ssm put-parameter \
  --name /kafka_spark_demo/kafka_servers \
  --type String \
  --value "<b-1.your-brokers.kafka.us-east-1.amazonaws.com:9098,b-2.your-brokers.kafka.us-east-1.amazonaws.com:9098>" \
  --description "Amazon MSK Kafka broker list" \
  --overwrite

aws ssm put-parameter \
  --name /kafka_spark_demo/kafka_demo_bucket \
  --type String \
  --value "<your-bucket-111222333444-us-east-1>" \
  --description "Amazon S3 bucket" \
  --overwrite