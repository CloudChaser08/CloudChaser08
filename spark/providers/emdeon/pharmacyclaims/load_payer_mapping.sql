-- Create a table for the payer_id to payer_name and payer_parent name mapping
DROP TABLE IF EXISTS payer_mapping;
CREATE EXTERNAL TABLE payer_mapping (
payer_id string,
payer_name string,
payer_parent_name string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://salusv/reference/emdeon/'
