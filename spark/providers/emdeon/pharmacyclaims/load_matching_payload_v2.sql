-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (
claimId string,
hvid string,
parentid string,
threeDigitZip string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'mapping.hvid' = 'hvid',
    'mapping.parentid' = 'parentId',
    'mapping.threeDigitZip' = 'threeDigitZip',
    'mapping.claimId' = 'claimId'
)
STORED AS TEXTFILE
LOCATION {matching_path};

-- Create a table for the matching payload data with entityId linked
DROP TABLE IF EXISTS matching_payload;
CREATE TEMPORARY VIEW matching_payload AS
SELECT
claimId as claimId,
COALESCE(mat.parentid, hvid) as hvid,
threeDigitZip
FROM
matching_payload_broken mat
CLUSTER BY claimId;
CACHE TABLE matching_payload;
