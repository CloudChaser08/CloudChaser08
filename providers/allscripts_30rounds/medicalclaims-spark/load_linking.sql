-- Create a table for linking claims to matching payloads
DROP TABLE IF EXISTS allscripts_linking;
CREATE TABLE allscripts_linking (
entityId string,
hvJoinKey string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '|'
)
STORED AS TEXTFILE
LOCATION {input_path}
