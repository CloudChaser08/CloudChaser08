DROP TABLE IF EXISTS transactional_tests;
CREATE EXTERNAL TABLE transactional_tests (
    hdr_rec_typ_cde          string,
    patient_id               string,
    test_order_id            string,
    status                   string,
    level_of_service         string,
    technology               string,
    specimen_type            string,
    body_site                string,
    icd_code                 string,
    specimen_collected_date  string,
    test_ordered_date        string,
    test_reported_date       string,
    test_canceled_date       string,
    panel_name               string,
    panel_code               string,
    test_name                string,
    test_code                string,
    client_zip               string
        ) 
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
