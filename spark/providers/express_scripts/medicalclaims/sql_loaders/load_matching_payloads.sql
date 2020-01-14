DROP TABLE IF EXISTS matching_payload;
CREATE EXTERNAL TABLE matching_payload (
    patientId   string,
    privateidone string,
    hvJoinKey   string
)
PARTITIONED BY (part_date_recv string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'mapping.patientId' = 'patientId',
    'mapping.privateidone' = 'privateIdOne',
    'mapping.hvJoinKey' = 'hvJoinKey'
)
STORED AS TEXTFILE
LOCATION {matching_path};
