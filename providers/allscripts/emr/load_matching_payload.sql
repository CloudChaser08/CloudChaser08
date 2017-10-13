DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
        personId varchar ENCODE lzo,
        hvid text ENCODE lzo,
        threeDigitZip char(3) ENCODE lzo,
        yearOfBirth text ENCODE lzo
        )
    DISTKEY(personId) SORTKEY(hvid);

COPY matching_payload FROM :matching_path CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/scratch/payloadpaths.json';
