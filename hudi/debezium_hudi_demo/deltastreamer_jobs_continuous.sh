export DATA_LAKE_BUCKET="open-data-lake-demo-us-east-1"

# Artists - CoW
spark-submit \
    --name "Upsert Artists - Hudi CoW" \
    --jars /usr/lib/spark/jars/spark-avro.jar,/usr/lib/hudi/hudi-utilities-bundle.jar \
    --conf spark.sql.catalogImplementation=hive \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer /usr/lib/hudi/hudi-utilities-bundle.jar \
    --table-type COPY_ON_WRITE \
    --source-ordering-field __source_ts_ms \
    --props "s3://${DATA_LAKE_BUCKET}/hudi/deltastreamer_artists_apicurio_cow.properties" \
    --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
    --target-base-path "s3://${DATA_LAKE_BUCKET}/moma/artists_cow/" \
    --target-table moma_cow.artists \
    --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
    --enable-sync \
    --continuous \
    --op UPSERT \
> /dev/null 2>&1 &

# Artworks - CoW
spark-submit \
    --name "Upsert Artworks - Hudi CoW" \
    --jars /usr/lib/spark/jars/spark-avro.jar,/usr/lib/hudi/hudi-utilities-bundle.jar \
    --conf spark.sql.catalogImplementation=hive \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer /usr/lib/hudi/hudi-utilities-bundle.jar \
    --table-type COPY_ON_WRITE \
    --source-ordering-field __source_ts_ms \
    --props "s3://${DATA_LAKE_BUCKET}/hudi/deltastreamer_artworks_apicurio_cow.properties" \
    --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
    --target-base-path "s3://${DATA_LAKE_BUCKET}/moma/artworks_cow/" \
    --target-table moma_cow.artworks \
    --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
    --enable-sync \
    --continuous \
    --op UPSERT \
> /dev/null 2>&1 &

# Artists - MoR
spark-submit \
    --name "Upsert Artworks - Hudi MoR" \
    --jars /usr/lib/spark/jars/spark-avro.jar,/usr/lib/hudi/hudi-utilities-bundle.jar \
    --conf spark.sql.catalogImplementation=hive \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer /usr/lib/hudi/hudi-utilities-bundle.jar \
    --table-type MERGE_ON_READ \
    --source-ordering-field __source_ts_ms \
    --props "s3://${DATA_LAKE_BUCKET}/hudi/deltastreamer_artists_apicurio_mor.properties" \
    --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
    --target-base-path "s3://${DATA_LAKE_BUCKET}/moma/artists_mor/" \
    --target-table moma_mor.artists \
    --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
    --enable-sync \
    --continuous \
    --op UPSERT \
> /dev/null 2>&1 &

# Artworks - MoR
spark-submit \
    --name "Upsert Artworks - Hudi MoR" \
    --jars /usr/lib/spark/jars/spark-avro.jar,/usr/lib/hudi/hudi-utilities-bundle.jar \
    --conf spark.sql.catalogImplementation=hive \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer /usr/lib/hudi/hudi-utilities-bundle.jar \
    --table-type MERGE_ON_READ \
    --source-ordering-field __source_ts_ms \
    --props "s3://${DATA_LAKE_BUCKET}/hudi/deltastreamer_artworks_apicurio_mor.properties" \
    --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
    --target-base-path "s3://${DATA_LAKE_BUCKET}/moma/artworks_mor/" \
    --target-table moma_mor.artworks \
    --schemaprovider-class org.apache.hudi.utilities.schema.SchemaRegistryProvider \
    --enable-sync \
    --continuous \
    --op UPSERT \
> /dev/null 2>&1 &

sleep 2

# list running processes
ps

# wait for them to fully start
sleep 30

# list running YARN applications
yarn application -list -appStates RUNNING -appTypes SPARK

# to kill all Spark jobs later (careful!)
# for x in $(yarn application -list -appTypes SPARK | awk 'NR > 2 { print $1 }'); do yarn application -kill $x; done