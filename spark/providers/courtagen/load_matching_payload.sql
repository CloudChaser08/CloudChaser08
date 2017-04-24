-- Create a table for the matching payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE EXTERNAL TABLE matching_payload_broken (
        hvJoinKey      string,
        claimid        string,
        hvid           string,
        parentid       string,
        threeDigitZip  char(3),
        yearOfBirth    string,
        gender         string,
        state          string,
        age            string
        )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
        'mapping.hvJoinKey' = 'hvJoinKey',
        'mapping.claimid' = 'claimId',
        'mapping.hvid' = 'hvid',
        'mapping.parentid' = 'parentId',
        'mapping.threeDigitZip' = 'threeDigitZip',
        'mapping.yearOfBirth' = 'yearOfBirth',
        'mapping.gender' = 'gender',
        'mapping.state' = 'state',
        'mapping.age' = 'age'
        )
STORED AS TEXTFILE
LOCATION {matching_path};

DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
        hvJoinKey      string,
        claimid        string,
        hvid           string,
        threeDigitZip  char(3),
        yearOfBirth    string,
        gender         string,
        state          string,
        age            string
)
INSERT INTO matching_payload
SELECT
        hvJoinKey,
        claimid,
        COALESCE(parentid, hvid),
        threeDigitZip,
        yearOfBirth,
        gender,
        state,
        age
FROM matching_payload_broken;
