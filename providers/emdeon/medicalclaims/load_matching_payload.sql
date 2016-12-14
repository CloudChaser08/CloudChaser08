-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (
claimId varchar ENCODE lzo,
hvid text ENCODE lzo,
parentid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo)
DISTKEY(claimId) SORTKEY(hvid);

-- Load matching payload data into table
copy matching_payload_broken from :matching_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/emdeon_new_payloadpaths.json';

-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
claimId varchar ENCODE lzo,
hvid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo)
DISTKEY(claimId) SORTKEY(claimId);

INSERT INTO matching_payload
SELECT
claimId,
COALESCE(mat.parentid, hvid),
threeDigitZip
FROM
matching_payload_broken mat;-- LEFT JOIN hvid_parentid par USING (hvid);
