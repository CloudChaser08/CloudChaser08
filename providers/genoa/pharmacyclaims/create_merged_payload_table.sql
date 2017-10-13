-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (hvJoinKey varchar ENCODE lzo,
hvid text ENCODE lzo,
threeDigitZip char(3) ENCODE lzo,
gender char(1) ENCODE lzo,
yearOfBirth char(4) ENCODE lzo) DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);
