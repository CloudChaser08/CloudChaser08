set hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec; 
set mapreduce.output.fileoutputformat.compress.type=BLOCK;
set spark.sql.shuffle.partitions=20;

DROP TABLE IF EXISTS express_script_enrollment_out;
CREATE TABLE express_script_enrollment_out
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n'
    LOCATION {out_path}
    AS SELECT
        obfuscate_hvid(hvid, 'UBC0') as hvid,
        source_version,
        patient_age,
        patient_year_of_birth,
        patient_zip,
        patient_state,
        patient_gender,
        source_record_id,
        source_record_qual,
        source_record_date,
        date_start,
        date_end,
        benefit_type
    FROM enrollmentrecords
    WHERE part_provider='express_scripts'
    DISTRIBUTE BY hvid;
