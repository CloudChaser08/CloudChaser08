DROP TABLE IF EXISTS transactions_trunk;
CREATE TABLE transactions_trunk (
        accn_id              text ENCODE lzo,
        dosid                text ENCODE lzo,
        local_order_code     text ENCODE lzo,
        standard_order_code  text ENCODE lzo,
        order_name           text ENCODE lzo,
        loinc_code           text ENCODE lzo,
        local_result_code    text ENCODE lzo,
        result_name          text ENCODE lzo
        ) DISTKEY(accn_id) SORTKEY(accn_id);

COPY transactions_trunk FROM :trunk_path CREDENTIALS :credentials DELIMITER '\t' BZIP2 EMPTYASNULL IGNOREHEADER 1 MAXERROR 10;

DROP TABLE IF EXISTS transactions_addon;
CREATE TABLE transactions_addon (
        accn_id              text ENCODE lzo,
        date_of_service      text ENCODE lzo,
        dosid                text ENCODE lzo,
        lab_id               text ENCODE lzo,
        date_collected       text ENCODE lzo,
        patient_first_name   text ENCODE lzo,
        patient_middle_name  text ENCODE lzo,
        patient_last_name    text ENCODE lzo,
        address1             text ENCODE lzo,
        address2             text ENCODE lzo,
        city                 text ENCODE lzo,
        state                text ENCODE lzo,
        zip_code             text ENCODE lzo,
        date_of_birth        text ENCODE lzo,
        patient_age          text ENCODE lzo,
        gender               text ENCODE lzo,
        diagnosis_code       text ENCODE lzo,
        icd_codeset_ind      text ENCODE lzo,
        hv_join_key          text ENCODE lzo
        ) DISTKEY(accn_id) SORTKEY(accn_id);

COPY transactions_addon FROM :addon_path CREDENTIALS :credentials DELIMITER '\t' BZIP2 EMPTYASNULL MAXERROR 10;

DROP TABLE IF EXISTS transactional_raw;
CREATE TABLE transactional_raw (
        hv_join_key          text ENCODE lzo,
        accn_id              text ENCODE lzo,
        dosid                text ENCODE lzo,
        local_order_code     text ENCODE lzo,
        standard_order_code  text ENCODE lzo,
        order_name           text ENCODE lzo,
        loinc_code           text ENCODE lzo,
        local_result_code    text ENCODE lzo,
        result_name          text ENCODE lzo,
        lab_id               text ENCODE lzo,
        date_of_service      text ENCODE lzo,
        date_collected       text ENCODE lzo,
        diagnosis_code       text ENCODE lzo,
        icd_codeset_ind      text ENCODE lzo
        ) DISTKEY(hv_join_key) SORTKEY(hv_join_key);

INSERT INTO transactional_raw 
SELECT addon.hv_join_key,
    trunk.accn_id,
    trunk.dosid,
    trunk.local_order_code,
    trunk.standard_order_code,
    trunk.order_name,
    trunk.loinc_code,
    trunk.local_result_code,
    trunk.result_name,
    addon.lab_id,
    addon.date_of_service,
    addon.date_collected,
    addon.diagnosis_code,
    addon.icd_codeset_ind
FROM transactions_trunk trunk
    INNER JOIN transactions_addon addon ON trunk.accn_id = addon.accn_id 
    AND trunk.dosid = addon.dosid
;
