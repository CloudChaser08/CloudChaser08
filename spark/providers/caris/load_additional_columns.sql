DROP TABLE IF EXISTS additional_columns;
CREATE EXTERNAL TABLE additional_columns (
        patient_id      string,
        ods_id          string,
        accession_date  string,
        sign_out_date   string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {addon_path}
;
