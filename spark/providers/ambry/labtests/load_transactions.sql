DROP TABLE IF EXISTS ambry_transactions;
CREATE EXTERNAL TABLE ambry_transactions (
    accession_number        string,
    final_test_name         string,
    test_category           string,
    genes_tested            string,
    first_name              string,
    last_name               string,
    gender                  string,
    date_of_birth           string,
    patient_zip_code        string,
    order_date              string,
    reported_date           string,
    ordering_provider       string,
    npi                     string,
    icd10_1                 string,
    icd10_2                 string,
    icd10_3                 string,
    icd10_4                 string,
    icd10_5                 string,
    icd10_6                 string,
    icd10_7                 string,
    icd10_8                 string,
    icd10_9                 string,
    icd10_10                string,
    icd10_11                string,
    icd10_12                string,
    hvJoinKey               string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;
