DROP TABLE IF EXISTS cardinal_dcoa_transactions;
CREATE EXTERNAL TABLE cardinal_dcoa_transactions (
    sk_utilization_key              string,
    sk_client_type_key              string,
    acq_cost                        string,
    qty                             string,
    extended_fee                    string,
    revenue                         string,
    extended_cost                   string,
    uom_qty                         string,
    dispense_dttm                   string,
    hdc                             string,
    effective_start_date            string,
    discharge_dttm                  string,
    patient_type                    string,
    client_patient_id               string,
    outlier                         string,
    physician_code                  string,
    prescribing_physician_code      string,
    ndc                             string,
    monthly_patient_days            string,
    discharge                       string,
    discharge_patient_days          string,
    total_patient_days              string,
    client_name                     string,
    address1                        string,
    drg_code                        string,
    service_area_description        string,
    master_service_area_description string,
    original_product_code_qualifier string,
    original_product_code           string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {input_path}
;
