DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
        hvJoinKey      text ENCODE lzo,
        claimid        varchar ENCODE lzo,
        hvid           text ENCODE lzo,
        parentid       text ENCODE lzo,
        threeDigitZip  char(3) ENCODE lzo,
        yearOfBirth    text ENCODE lzo,
        gender         text ENCODE lzo,
        state          text ENCODE lzo,
        age            text ENCODE lzo
        ) DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

COPY matching_payload FROM :matching_path CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/quest-normalization/payloadpaths.json';

