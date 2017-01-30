-- raw payload data
DROP TABLE IF EXISTS matching_payload_dupes CASCADE;
CREATE TABLE matching_payload_dupes (
        claimid        varchar ENCODE lzo,
        hvid           text ENCODE lzo,
        parentid       text ENCODE lzo,
        threeDigitZip  char(3) ENCODE lzo,
        yearOfBirth    text ENCODE lzo,
        gender         text ENCODE lzo,
        state          text ENCODE lzo,
        age            text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

COPY matching_payload_dupes FROM :matching_path CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/payloadpaths.json';

DROP TABLE IF EXISTS matching_payload CASCADE;
CREATE TABLE matching_payload (
        claimid        varchar ENCODE lzo,
        hvid           text ENCODE lzo,
        parentid       text ENCODE lzo,
        threeDigitZip  char(3) ENCODE lzo,
        yearOfBirth    text ENCODE lzo,
        gender         text ENCODE lzo,
        state          text ENCODE lzo,
        age            text ENCODE lzo
        ) DISTKEY(claimid) SORTKEY(claimid);

INSERT INTO matching_payload (select distinct * from matching_payload_dupes);
DROP TABLE matching_payload_dupes CASCADE;
