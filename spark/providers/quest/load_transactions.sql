DROP TABLE IF EXISTS transactions_raw_noprov;
CREATE EXTERNAL TABLE transactions_raw_noprov (
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

DROP TABLE IF EXISTS transactions_provider_addon_dupes;
CREATE EXTERNAL TABLE transactions_provider_addon_dupes (
        accn_id              string,
        dosid                string,
        lab_code             string,
        acct_zip             string,
        npi                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '\t'
        )
    STORED AS TEXTFILE
    LOCATION {prov_addon_path}
    ;

DROP TABLE IF EXISTS transactions_provider_addon;
CREATE TABLE transactions_provider_addon
AS SELECT DISTINCT * FROM transactions_provider_addon_dupes;

DROP VIEW IF EXISTS transactional_raw;
CREATE VIEW transactional_raw AS
SELECT DISTINCT
    trunk.accn_id,
    trunk.dosid,
    trunk.local_order_code,
    trunk.standard_order_code,
    trunk.order_name,
    trunk.loinc_code,
    trunk.local_result_code,
    trunk.result_name,
    trunk.lab_id,
    trunk.date_of_service,
    trunk.date_collected,
    trunk.diagnosis_code,
    trunk.icd_codeset_ind,
    prov_addon.acct_zip,
    prov_addon.npi
FROM transactions_raw_noprov trunk
    LEFT JOIN transactions_provider_addon prov_addon ON trunk.accn_id = prov_addon.accn_id
    AND trunk.dosid = prov_addon.dosid
;
