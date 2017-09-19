SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET spark.sql.shuffle.partitions=1;
DROP TABLE IF EXISTS cardinal_deliverable;
CREATE TABLE cardinal_deliverable
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )  
    STORED AS TEXTFILE
AS SELECT * FROM pharmacyclaims_common_model
DISTRIBUTE BY hvid;
SET spark.sql.shuffle.partitions={partitions};
