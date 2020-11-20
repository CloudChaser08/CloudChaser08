DROP TABLE IF EXISTS matching_payload;
CREATE EXTERNAL TABLE matching_payload (
    patientId   string,
    hvJoinKey   string
)
PARTITIONED BY (part_date_recv string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "ignore.malformed.json" = "true",
    'mapping.patientId' = 'patientId',
    'mapping.hvJoinKey' = 'hvJoinKey'
)
STORED AS TEXTFILE
LOCATION {matching_path};
