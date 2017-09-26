set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec; 
set mapreduce.output.fileoutputformat.compress.type=BLOCK;

-- set new partition count to control number of files output
set spark.sql.shuffle.partitions={unload_partition_count};

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO final_unload
{select_statement}
DISTRIBUTE BY {distribution_key}
;

-- reset to old partition count
set spark.sql.shuffle.partitions={original_partition_count};
;
