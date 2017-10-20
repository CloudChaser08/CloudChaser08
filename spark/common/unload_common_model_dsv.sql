SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET spark.sql.shuffle.partitions={num_files};
DROP TABLE IF EXISTS unload_dsv;
CREATE TABLE unload_dsv
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "{delimiter}"
    )  
    STORED AS TEXTFILE
    LOCATION {location}
AS SELECT * FROM {table_name}
DISTRIBUTE BY rand();

-- reset LOCATION to avoid complications that could arise when
-- dropping this table
ALTER TABLE unload_dsv SET LOCATION '/';

SET spark.sql.shuffle.partitions={original_partition_count};
