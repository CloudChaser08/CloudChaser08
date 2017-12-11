DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
        icd10code  string,
        authorid   string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
