-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (hvJoinKey varchar ENCODE lzo,
hvid text ENCODE lzo,
parentId text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo,
gender char(1) ENCODE lzo,
yearOfBirth char(4) ENCODE lzo) DISTKEY(hvJoinKey) SORTKEY(hvid);

-- Load matching payload data into table
copy matching_payload_broken from :matching_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/navicure_payloadpaths.json';

-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (hvJoinKey varchar ENCODE lzo,
hvid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo,
gender char(1) ENCODE lzo,
yearOfBirth char(4) ENCODE lzo) DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

INSERT INTO matching_payload
SELECT
hvJoinKey,
COALESCE(mat.parentid, hvid),
threeDigitZip,
gender,
yearOfBirth
FROM
matching_payload_broken mat;
