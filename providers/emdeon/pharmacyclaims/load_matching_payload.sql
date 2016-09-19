-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (claimid varchar ENCODE lzo,
hvid text ENCODE lzo,
parentid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo) DISTKEY(claimid) SORTKEY(claimid);

-- Load matching payload data into table
copy matching_payload from :matching_path credentials :credentials BZIP2 format as JSON 's3://healthveritydev/ifishbein/payloadpaths.json';
