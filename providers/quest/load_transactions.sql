DROP TABLE IF EXISTS transactional_raw;
CREATE TABLE transactional_raw (
        accn_id              text ENCODE lzo,
        dosid                text ENCODE lzo,
        local_order_code     text ENCODE lzo,
        standard_order_code  text ENCODE lzo,
        order_name           text ENCODE lzo,
        loinc_code           text ENCODE lzo,
        local_result_code    text ENCODE lzo,
        result_name          text ENCODE lzo,
        date_of_service      text ENCODE lzo,
        date_collected       text ENCODE lzo,
        diagnosis_code       text ENCODE lzo,
        icd_codeset_ind      text ENCODE lzo
        );

COPY transactional_raw FROM :input_path CREDENTIALS :credentials BZIP2 EMPTYASNULL DELIMITER '|';
