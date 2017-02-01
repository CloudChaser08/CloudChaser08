-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (
hvid text ENCODE lzo,
parentid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo,
isMultimatch boolean,
hvJoinKey text ENCODE lzo
)
DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

-- Load matching payload data into table
copy matching_payload_broken from :matching_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/allscripts_payloadpaths.json';

-- Create a table for the matching payload data with entityId linked
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
entityId varchar ENCODE lzo,
hvid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo
)
DISTKEY(entityId) SORTKEY(entityId);

INSERT INTO matching_payload
SELECT
entityId,
CASE WHEN isMultimatch THEN COALESCE(mat.parentid, hvid) || '_mm' ELSE COALESCE(mat.parentid, hvid) END,
threeDigitZip
FROM
matching_payload_broken mat INNER JOIN allscripts_linking USING (hvJoinKey);
