DROP TABLE IF EXISTS f_diagnosis;
CREATE TABLE f_diagnosis (
        patient_key   text ENCODE lzo,
        provider_key  text ENCODE lzo,
        problem_icd   text ENCODE lzo,
        icd_type      text ENCODE lzo,
        snomed        text ENCODE lzo,
        costar_key    text ENCODE lzo
        ) DISTKEY(patient_key) SORTKEY(patient_key);

COPY f_diagnosis FROM 's3://healthveritydev/musifer/amazingchartstransactional/f_diagnosis.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS f_injection;
CREATE TABLE f_injection (
        patient_key      text ENCODE lzo,
        provider_key     text ENCODE lzo,
        vaccine_cpt_key  text ENCODE lzo,
        cpt              text ENCODE lzo
        ) DISTKEY(patient_key) SORTKEY(patient_key);

COPY f_injection FROM 's3://healthveritydev/musifer/amazingchartstransactional/f_injection.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS f_medication;
CREATE TABLE f_medication (
        patient_key           text ENCODE lzo,
        drug_key              text ENCODE lzo,
        prescribing_provider  text ENCODE lzo
        ) DISTKEY(patient_key) SORTKEY(patient_key);

COPY f_medication FROM 's3://healthveritydev/musifer/amazingchartstransactional/f_medication.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS f_procedure;
CREATE TABLE f_procedure (
        patient_key   text ENCODE lzo,
        provider_key  text ENCODE lzo,
        cpt_key       text ENCODE lzo
        ) DISTKEY(patient_key) SORTKEY(patient_key);

COPY f_procedure FROM 's3://healthveritydev/musifer/amazingchartstransactional/f_procedure.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS f_lab;
CREATE TABLE f_lab (
        patient_key           text ENCODE lzo,
        ordering_provider_id  text ENCODE lzo,
        loinc_test_code       text ENCODE lzo,
        lab_directory_key     text ENCODE lzo
        ) DISTKEY(patient_key) SORTKEY(patient_key);

COPY f_lab FROM 's3://healthveritydev/musifer/amazingchartstransactional/f_lab.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS d_drug;
CREATE TABLE d_drug (
        drug_key  text ENCODE lzo,
        drug_id   text ENCODE lzo,
        ndc       text ENCODE lzo
        ) DISTSTYLE ALL;

COPY d_drug FROM 's3://healthveritydev/musifer/amazingchartstransactional/d_drug.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS d_cpt;
CREATE TABLE d_cpt (
        cpt_key   text ENCODE lzo,
        cpt_code  text ENCODE lzo
        ) DISTSTYLE ALL;

COPY d_cpt FROM 's3://healthveritydev/musifer/amazingchartstransactional/d_cpt.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS d_lab_directory;
CREATE TABLE d_lab_directory (
        lab_directory_key  text ENCODE lzo,
        test_name          text ENCODE lzo
        ) DISTSTYLE ALL;

COPY d_lab_directory FROM 's3://healthveritydev/musifer/amazingchartstransactional/d_lab_directory.txt' CREDENTIALS :credentials EMPTYASNULL IGNOREHEADER 1 DELIMITER '\t';

DROP TABLE IF EXISTS matching_payload;
CREATE TABLE matching_payload (
        hvid         text ENCODE lzo,
        parentId     text ENCODE lzo,
        childId      text ENCODE lzo,
        personId     text ENCODE lzo,
        state        text ENCODE lzo,
        gender       text ENCODE lzo,
        yearOfBirth  text ENCODE lzo
        ) DISTKEY(personId) SORTKEY(personId);

COPY matching_payload FROM 's3://salusv/matching/prod/payload/f00ca57c-4935-494e-9e40-b064fd38afda/HV-DEID-20160610.decrypted.json.2016-09-16T16-56-17.json.bz2' CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/ac_payloadpaths.json';

DROP TABLE IF EXISTS parent_child_map;
CREATE TABLE parent_child_map (
        hvid         text ENCODE lzo,
        parentId     text ENCODE lzo
        ) DISTKEY(hvid) SORTKEY(hvid);

COPY parent_child_map FROM 's3://salusv/matching/fetch-parent-ids/payload/fetch-parent-ids/20160705_Claims_US_CF_Hash_File_HV_Encrypt.dat.decrypted.json2016-10-13T22-06-23.0-1000000.json.bz2' CREDENTIALS :credentials BZIP2 FORMAT AS JSON 's3://healthveritydev/musifer/parent-child-payloadpaths.json';

DROP TABLE IF EXISTS full_transactional;
CREATE TABLE full_transactional (
    record_id       bigint IDENTITY(0,1),
    hvid            text ENCODE lzo,
    patient_gender  text ENCODE lzo,
    patient_state   text ENCODE lzo,
    patient_age     text ENCODE lzo,
    drug            text ENCODE lzo,
    diagnosis       text ENCODE lzo,
    lab             text ENCODE lzo,
    procedure       text ENCODE lzo
    )

