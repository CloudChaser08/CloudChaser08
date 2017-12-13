DROP TABLE IF EXISTS transactions_trunk_dupes;
CREATE EXTERNAL TABLE transactions_trunk_dupes (
        accn_id              string,
        dosid                string,
        local_order_code     string,
        standard_order_code  string,
        order_name           string,
        loinc_code           string,
        local_result_code    string,
        result_name          string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '\t'
        )
    STORED AS TEXTFILE
    LOCATION {trunk_path}
    ;

DROP TABLE IF EXISTS transactions_trunk;
CREATE TABLE transactions_trunk
AS SELECT DISTINCT * FROM transactions_trunk_dupes;

DROP TABLE IF EXISTS transactions_addon_dupes;
CREATE EXTERNAL TABLE transactions_addon_dupes (
        accn_id              string,
        date_of_service      string,
        dosid                string,
        lab_id               string,
        date_collected       string,
        patient_first_name   string,
        patient_middle_name  string,
        patient_last_name    string,
        address1             string,
        address2             string,
        city                 string,
        state                string,
        zip_code             string,
        date_of_birth        string,
        patient_age          string,
        gender               string,
        diagnosis_code       string,
        icd_codeset_ind      string,
        acct_zip             string,
        npi                  string,
        hv_join_key          string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '\t'
        )
    STORED AS TEXTFILE
    LOCATION {addon_path}
    ;

DROP TABLE IF EXISTS transactions_addon;
CREATE TABLE transactions_addon
AS SELECT DISTINCT * FROM transactions_addon_dupes;

DROP VIEW IF EXISTS transactional_raw;
CREATE VIEW transactional_raw AS
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
    addon.icd_codeset_ind,
    addon.acct_zip,
    addon.npi
FROM transactions_trunk trunk
    INNER JOIN transactions_addon addon ON trunk.accn_id = addon.accn_id
    AND trunk.dosid = addon.dosid
;
