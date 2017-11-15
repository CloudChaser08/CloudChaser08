DROP TABLE IF EXISTS demographics_transactions_duplicates;
CREATE TABLE demographics_transactions_duplicates (
        demographicid        string,
        first_name           string,
        middle_name          string,
        last_name            string,
        prefix               string,
        suffix               string,
        ssn                  string,
        dob                  string,
        other_id             string,
        account_id           string,
        gender               string,
        race                 string,
        ethnicity            string,
        primary_language     string,
        phone_number         string,
        phone_ext            string,
        phone_type           string,
        second_number        string,
        second_ext           string,
        second_type          string,
        first_email          string,
        second_email         string,
        death_indication     string,
        death_date           string,
        address_type         string,
        address_line_1       string,
        address_line_2       string,
        city                 string,
        state                string,
        zip                  string,
        hj_create_timestamp  string,
        hj_modify_timestamp  string,
        hvJoinKey            string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {demographics_path}
    ;

DROP TABLE IF EXISTS demographics_transactions;
CREATE TABLE demographics_transactions AS
SELECT dem.*
FROM demographics_transactions_duplicates dem
    INNER JOIN (
    SELECT demographicid, COUNT(*) AS row_cnt
    FROM demographics_transactions_duplicates
    GROUP BY demographicid
        ) cnt ON dem.demographicid = cnt.demographicid
WHERE cnt.row_cnt = 1
    ;

DROP TABLE IF EXISTS cpt_transactions;
CREATE TABLE cpt_transactions (
        id    string,
        date  string,
        cpt   string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {cpt_path}
    ;

DROP TABLE IF EXISTS diagnosis_transactions;
CREATE TABLE diagnosis_transactions (
        id    string,
        date  string,
        diag  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {diag_path}
    ;

DROP TABLE IF EXISTS loinc_transactions;
CREATE TABLE loinc_transactions (
        id     string,
        date   string,
        loinc  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {loinc_path}
    ;

DROP TABLE IF EXISTS ndc_transactions;
CREATE TABLE ndc_transactions (
        id    string,
        date  string,
        ndc   string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {ndc_path}
    ;
