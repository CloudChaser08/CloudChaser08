SET spark.sql.shuffle.partitions=1;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE IF EXISTS cardinal_mpi_model;
CREATE TABLE cardinal_mpi_model
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )
    STORED AS TEXTFILE
    LOCATION {location}
AS SELECT
    slightly_obfuscate_hvid(cast(hvid as integer), 'Cardinal_MPI-0') as hvid,
    claimId,
    multiMatchQuality
FROM matching_payload
DISTRIBUTE BY 1
;

