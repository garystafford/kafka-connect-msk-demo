#!/bin/bash

# Purpose: EMR bootstrap script
# Author:  Gary A. Stafford
# Date: 2021-09-10

# arg passed in by CloudFormation
if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
fi

SPARK_BUCKET=$1

# update yum packages, install jq
sudo yum update -y
sudo yum install -y jq

# jsk truststore for connecting to msk
sudo cp /usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/lib/security/cacerts \
  /tmp/kafka.client.truststore.jks

# set region for boto3
aws configure set region \
  "$(curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)"

# install python packages for pyspark scripts
sudo python3 -m pip install boto3 botocore ec2-metadata

# install required jars for spark
sudo aws s3 cp \
  "s3://${SPARK_BUCKET}/jars/" /usr/lib/spark/jars/ \
  --recursive --exclude "*" --include "*.jar"