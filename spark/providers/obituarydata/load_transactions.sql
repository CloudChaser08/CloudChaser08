DROP TABLE IF EXISTS transactional_raw;
CREATE EXTERNAL TABLE transactional_raw (
        id                     string,
        last_name              string,
        first_name             string,
        middle_init            string,
        date_of_death          string,
        date_of_birth          string,
        age                    string,
        suffix                 string,
        city                   string,
        state                  string,
        date_of_record_entry   string,
        date_of_record_update  string,
        hv_join_key            string
        ) 
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
