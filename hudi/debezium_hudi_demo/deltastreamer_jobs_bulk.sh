export DATA_LAKE_BUCKET="open-data-lake-demo-us-east-1"

# drop previous copies of databases in Hive
hive -e "DROP DATABASE moma_cow CASCADE;DROP DATABASE moma_mor CASCADE;" || echo "Databases do not exist"

# Artists - CoW
spark-submit \
    --name "Bulk Insert Artists - Hudi CoW" \
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
    --op BULK_INSERT \
    --filter-dupes

# Artworks - CoW
spark-submit \
    --name "Bulk Insert Artworks - Hudi CoW" \
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
    --op BULK_INSERT \
    --filter-dupes

# Artists - MoR
spark-submit \
    --name "Bulk Insert Artists - Hudi MoR" \
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
    --op BULK_INSERT \
    --filter-dupes

# Artworks - MoR
spark-submit \
    --name "Bulk Insert Artworks - Hudi MoR" \
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
    --op BULK_INSERT \
    --filter-dupes

# confirm results in Hive
hive -e "SHOW DATABASES;use moma_cow;SHOW TABLES;use moma_mor;SHOW TABLES;"

# update Hive Partitions
hive -e "MSCK REPAIR TABLE moma_mor.artists_ro;MSCK REPAIR TABLE moma_mor.artists_rt;"
hive -e "MSCK REPAIR TABLE moma_mor.artworks_ro;MSCK REPAIR TABLE moma_mor.artworks_rt;"
hive -e "MSCK REPAIR TABLE moma_cow.artists;MSCK REPAIR TABLE moma_cow.artworks;"
