DROP TABLE IF EXISTS exploded_payload;
CREATE TABLE exploded_payload (
        pk             text ENCODE lzo,
        hvid           text ENCODE lzo,
        parentId       text ENCODE lzo,
        childId        text ENCODE lzo,
        personId       text ENCODE lzo,
        threeDigitZip  text ENCODE lzo,
        gender         text ENCODE lzo,
        yearOfBirth    text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);

COPY exploded_payload FROM 's3://healthveritydev/musifer/caris/exploded.json.bz2' CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/payloadpaths/caris_explodedpayloadpaths.json';

DROP TABLE IF EXISTS parent_child_map;
CREATE TABLE parent_child_map (
        hvid         text ENCODE lzo,
        parentId     text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);

COPY parent_child_map FROM 's3://salusv/matching/fetch-parent-ids/payload/fetch-parent-ids/20160705_Claims_US_CF_Hash_File_HV_Encrypt.dat.decrypted.json2016-10-13T22-06-23.0-1000000.json.bz2' CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/payloadpaths/parent-child-payloadpaths.json';

DROP TABLE IF EXISTS full_exploded_payload;
CREATE TABLE full_exploded_payload (
        hvid         text ENCODE lzo,
        personId     text ENCODE lzo,
        state        text ENCODE lzo,
        gender       text ENCODE lzo,
        yearOfBirth  text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);
