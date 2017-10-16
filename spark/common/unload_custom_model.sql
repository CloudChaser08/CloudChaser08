set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec; 
set mapreduce.output.fileoutputformat.compress.type=BLOCK;
set spark.sql.shuffle.partitions={partitions};
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO final_unload
{select_statement}
DISTRIBUTE BY rand()
;
-- Just in case, change the location of the table before dropping it
-- This will prevent data in the original location from being deleted
ALTER TABLE final_unload SET LOCATION '/';
DROP TABLE final_unload;
