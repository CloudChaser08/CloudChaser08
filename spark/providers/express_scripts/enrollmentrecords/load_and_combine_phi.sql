SET spark.sql.shuffle.partitions=20;
SET parquet.compression=GZIP;
set spark.sql.parquet.compression.codec=gzip;
SET hive.exec.compress.output=true;
set mapreduce.output.fileoutputformat.compress=true;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
set mapreduce.output.fileoutputformat.compress.type=BLOCK;
DROP TABLE IF EXISTS esi_phi;
CREATE EXTERNAL TABLE esi_phi (
    hvid            string, 
    zip             string,
    gender          string,
    year_of_birth   string,
    patient_id      string
)
STORED AS PARQUET
LOCATION {s3_phi_path};

DROP TABLE IF EXISTS local_phi;
CREATE TABLE local_phi (
    hvid            string, 
    zip             string,
    gender          string,
    year_of_birth   string,
    patient_id      string
)
STORED AS PARQUET
LOCATION {local_phi_path};
INSERT INTO local_phi
SELECT DISTINCT * FROM (
    SELECT * FROM esi_phi
    UNION ALL
    SELECT hvid, threeDigitZip, gender, yearOfBirth, patientId FROM new_phi
)
DISTRIBUTE BY patient_id;
