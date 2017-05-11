SET spark.sql.parquet.compression.codec=gzip;
SET spark.sql.shuffle.partitions={partitions};
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO final_unload
{select_statement}
DISTRIBUTE BY record_id;
