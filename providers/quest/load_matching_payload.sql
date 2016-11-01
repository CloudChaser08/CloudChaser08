-- parent-child mapping to fix parent child hvid bug
DROP TABLE IF EXISTS parent_child_map;
CREATE TABLE parent_child_map (
        hvid         text ENCODE lzo,
        parentId     text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);

COPY parent_child_map FROM 's3://salusv/matching/fetch-parent-ids/payload/fetch-parent-ids/20160705_Claims_US_CF_Hash_File_HV_Encrypt.dat.decrypted.json2016-10-13T22-06-23.0-1000000.json.bz2' CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/parent-child-payloadpaths.json';xb

-- raw payload data
DROP TABLE IF EXISTS matching_payload_broken;
CREATE TABLE matching_payload_broken (
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

COPY matching_payload_broken FROM :matching_path CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/quest-normalization/payloadpaths.json';

-- Insert fixed matching payload data
DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
        hvJoinKey      text ENCODE lzo,
        claimid        varchar ENCODE lzo,
        hvid           text ENCODE lzo,
        threeDigitZip  char(3) ENCODE lzo,
        yearOfBirth    text ENCODE lzo,
        gender         text ENCODE lzo,
        state          text ENCODE lzo,
        age            text ENCODE lzo
        ) DISTKEY(hvJoinKey) SORTKEY(hvJoinKey);

INSERT INTO matching_payload
SELECT mat.hvJoinKey,
    mat.claimid,
    COALESCE(par.parentid, COALESCE(par.hvid, mat.hvid)),
    mat.threeDigitZip,
    mat.yearOfBirth,
    mat.gender,
    mat.state,
    mat.age
FROM
    matching_payload_broken mat 
    LEFT JOIN parent_child_map par USING (hvid);
