# Demonstration Commands

The following commands were used during the demonstration.

Set-up variables

```shell
export AWS_ACCOUNT=$(aws sts get-caller-identity --output text --query 'Account')
export EKS_REGION="us-east-1"
export CLUSTER_NAME="<your_eks_cluster>"
export NAMESPACE="kafka"
export EMR_MASTER="<your_emr_master_node>"
export EMR_KEY="~/.ssh/<your_ssh_key>.pem"
export DATA_LAKE_BUCKET="<your_data_lake_bucket>"
```

Install Hudi DeltaStreamer dependencies

```shell
aws s3 cp base.properties "s3://${DATA_LAKE_BUCKET}/hudi/"
aws s3 cp deltastreamer_s3.properties "s3://${DATA_LAKE_BUCKET}/hudi/"
aws s3 cp moma.public.artists-value.avsc "s3://${DATA_LAKE_BUCKET}/hudi/"
```

Shell into Kafka Connect container

```shell
export KAFKA_CONTAINER=$(
  kubectl get pods -n kafka -l app=kafka-connect-msk | \
    awk 'FNR == 2 {print $1}')

kubectl exec -it $KAFKA_CONTAINER -n kafka -c kafka-connect-msk -- bash
```

Log into EMR Master Node

```shell
ssh -i ${EMR_KEY} hadoop@${EMR_MASTER}
```

Prerequisites for Apache Hudi (see AWS EMR/Hudi docs)

```shell
hdfs dfs -mkdir -p /apps/hudi/lib
hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar
hdfs dfs -copyFromLocal /usr/lib/spark/jars/spark-avro.jar /apps/hudi/lib/spark-avro.jar
```

Drop Hive database using EMR Master Node

```hiveql
SHOW DATABASES;
DROP TABLE artists_ro;
DROP TABLE artists_rt;
DROP DATABASE moma;
SHOW DATABASES;
```

Delete Existing Kafka Connect and CDC Kafka topics

```shell
bin/kafka-topics.sh \
    --delete \
    --topic connect-configs \
    --bootstrap-server $BBROKERS \
    --command-config config/client-iam.properties

bin/kafka-topics.sh \
    --delete \
    --topic connect-offsets \
    --bootstrap-server $BBROKERS \
    --command-config config/client-iam.properties

bin/kafka-topics.sh \
    --delete \
    --topic connect-status \
    --bootstrap-server $BBROKERS \
    --command-config config/client-iam.properties

bin/kafka-topics.sh \
    --delete \
    --topic moma.* \
    --bootstrap-server $BBROKERS \
    --command-config config/client-iam.properties
```

Start Kafka Connect as background process

```shell
bin/connect-distributed.sh config/connect-distributed.properties > /dev/null 2>&1 &
tail -f logs/connect.log
```

Deploy Source and Sink Connectors

```shell
curl -s -d @"config/debezium_avro_source_connector_postgresql_moma.json" \
  -H "Content-Type: application/json" \
  -X PUT http://localhost:8083/connectors/debezium_avro_source_connector_postgresql_moma/config | jq

curl -s -d @"config/s3_sink_connector_debezium_avro_moma.json" \
  -H "Content-Type: application/json" \
  -X PUT http://localhost:8083/connectors/s3_sink_connector_debezium_avro_moma/config | jq

curl -s -X GET http://localhost:8083/connectors | jq

curl -s -H "Content-Type: application/json" \
    -X GET http://localhost:8083/connectors/debezium_avro_source_connector_postgresql_moma/status | jq

curl -s -H "Content-Type: application/json" \
    -X GET http://localhost:8083/connectors/s3_sink_connector_debezium_avro_moma/status | jq
```

Show Kafka Topics

```shell
# list topic
bin/kafka-topics.sh --list \
  --bootstrap-server $BBROKERS \
  --command-config config/client-iam.properties

bin/kafka-console-consumer.sh \
  --topic moma.public.artists \
  --from-beginning --max-messages 10 \
  --property print.key=true \
  --property print.value=true \
  --property print.offset=true \
  --property print.partition=true \
  --property print.headers=true \
  --property print.timestamp=true \
  --bootstrap-server $BBROKERS \
  --consumer.config config/client-iam.properties
```

Show Avro file

```shell
java -jar avro-tools-1.10.2.jar count moma.public.artists+0+0000000000.avro

java -jar avro-tools-1.10.2.jar tojson \
  --pretty --head 2 moma.public.artists+0+0000000000.avro | jq
```

Run Hudi DeltaStreamer continuously

```shell
export DATA_LAKE_BUCKET="<your_data_lake_bucket>"
spark-submit --jars /usr/lib/spark/jars/spark-avro.jar,/usr/lib/hudi/hudi-utilities-bundle.jar \
    --conf spark.sql.catalogImplementation=hive \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer /usr/lib/hudi/hudi-utilities-bundle.jar \
    --table-type MERGE_ON_READ \
    --source-ordering-field __source_ts_ms \
    --props "s3://${DATA_LAKE_BUCKET}/hudi/deltastreamer_s3.properties" \
    --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
    --target-base-path "s3://${DATA_LAKE_BUCKET}/moma/artists/" \
    --target-table moma.artists \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --enable-sync \
    --continuous \
    --op UPSERT
```

Preview Hive database/table/rows

```hiveql
SHOW databases;

USE moma;

SHOW TABLES;

DESCRIBE artists_ro;
DESCRIBE artists_rt;

SELECT *
FROM artists_ro
LIMIT 10;

SELECT *
FROM artists_ro
WHERE artist_id = (
    SELECT artist_id
    FROM artists_ro
    WHERE `__deleted` = 'true'
    LIMIT 1);
```

Make SQL changes to Artists table

```sql
--upserts
UPDATE public.artists
SET nationality = 'Japanese',
    gender      = 'Male'
WHERE artist_id = 201;

UPDATE public.artists
SET birth_year = 1845
WHERE artist_id = 266;

UPDATE public.artists
SET birth_year = 1908,
    death_year = 1988
WHERE artist_id = 299;

-- delete
DELETE
FROM public.artists
WHERE artist_id = 568;
```