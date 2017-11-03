DROP TABLE IF EXISTS transactions;
CREATE EXTERNAL TABLE transactions (
        patient_first_name  string,
        patient_last_name   string,
        patient_dob         string,
        patient_sex         string,
        patient_zip5        string,
        patient_st_addr1    string,
        patient_st_addr2    string,
        patient_city        string,
        patient_st          string,
        member_id           string,
        group_id            string,
        patient_id          string,
        test_name           string,
        test_number         string,
        loinc_code          string,
        normal_dec_low      string,
        normal_dec_high     string,
        result_dec          string,
        result_abn_code     string,
        result_abbrev       string,
        pat_dos             string,
        perf_lab_code       string,
        test_ordered_code   string,
        test_ordered_name   string,
        patient_lpid        string,
        hvjoinkey           string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES 
    (
        "input.regex" = "^(.{{9}})(.{{12}})(.{{8}})(.{{1}})(.{{5}})(.{{55}})(.{{55}})(.{{30}})(.{{2}})(.{{15}})(.{{30}})(.{{20}})(.{{30}})(.{{6}})(.{{7}})(.{{11}})(.{{11}})(.{{12}})(.{{1}})(.{{12}})(.{{10}})(.{{5}})(.{{6}})(.{{30}})(.{{16}})(.*)"
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
