-- parent-child mapping to fix parent child hvid bug
DROP TABLE IF EXISTS parent_child_map;
CREATE EXTERNAL TABLE parent_child_map (
    hvid         string,
    parentId     string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3a://healthveritydev/ifishbein/hvid_parentid/'

