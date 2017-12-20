DROP TABLE IF EXISTS mckesson_macrohelix_transactions;
CREATE EXTERNAL TABLE mckesson_macrohelix_transactions(
    row_id                      string,
    service_date                string,
    mr_num                      string,
    visit_num                   string,
    first_name                  string,
    last_name                   string,
    birth_date                  string,
    patient_gender              string,
    patient_zip                 string,
    patient_type                string,
    ndc                         string,
    bupp                        string,
    packages                    string,
    quantity                    string,
    inpatient_flag              string,
    medicaid_flag               string,
    orphan_drug_flag            string,
    insurance                   string,
    insurance_2                 string,
    insurance_3                 string,
    jcode                       string,
    gross_charge                string,
    hospital_state              string,
    hospital_zip                string,
    dx_01                       string,
    dx_02                       string,
    dx_03                       string,
    dx_04                       string,
    dx_05                       string,
    dx_06                       string,
    dx_07                       string,
    dx_08                       string,
    dx_09                       string,
    dx_10                       string,
    dx_11                       string,
    dx_12                       string,
    dx_13                       string,
    dx_14                       string,
    dx_15                       string,
    dx_16                       string,
    dx_17                       string,
    dx_18                       string,
    dx_19                       string,
    dx_20                       string,
    dx_21                       string,
    dx_22                       string,
    dx_23                       string,
    dx_24                       string,
    hvJoinKey                   string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;

