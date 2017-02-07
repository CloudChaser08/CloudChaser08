-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (
hvid string,
parentid string,
threeDigitZip char(3),
isMultimatch boolean,
hvJoinKey string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'mapping.hvid' = 'hvid',
    'mapping.parentid' = 'parentId',
    'mapping.threeDigitZip' = 'threeDigitZip',
    'mapping.isMultimatch' = 'multiMatch',
    'mapping.hvJoinKey' = 'hvJoinKey'
)
STORED AS TEXTFILE
LOCATION {matching_path};

-- Create a table for the matching payload data with entityId linked
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
entityId varchar,
hvid string,
threeDigitZip char(3) 
);

INSERT INTO matching_payload
SELECT
entityId,
CASE WHEN isMultimatch THEN concat(COALESCE(mat.parentid, hvid), '_mm') ELSE COALESCE(mat.parentid, hvid) END,
threeDigitZip
FROM
matching_payload_broken mat INNER JOIN allscripts_linking USING (hvJoinKey);
