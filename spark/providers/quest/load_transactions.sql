DROP TABLE IF EXISTS transactional_raw;
CREATE EXTERNAL TABLE transactional_raw (
        accn_id              string,
        dosid                string,
        local_order_code     string,
        standard_order_code  string,
        order_name           string,
        loinc_code           string,
        local_result_code    string,
        result_name          string,
        lab_id               string,
        date_of_service      string,
        date_collected       string,
        diagnosis_code       string,
        icd_codeset_ind      string
        ) 
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
