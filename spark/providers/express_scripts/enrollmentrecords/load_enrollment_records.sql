DROP TABLE IF EXISTS enrollment_records;
CREATE EXTERNAL TABLE enrollment_records (
    patient_id      string,
    start_date      string,
    end_date        string,
    operation_date  string,
    status          string,
    hv_join_key     string
)
PARTITIONED BY (part_date_recv string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES
(
    "input.regex" = "^(.{{15}})(.{{8}})(.{{8}})(.{{8}})(.{{1}})(.{{36}})"
)
STORED AS TEXTFILE
LOCATION {input_path};
