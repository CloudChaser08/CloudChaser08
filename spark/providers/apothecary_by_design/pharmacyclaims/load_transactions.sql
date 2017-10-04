DROP TABLE IF EXISTS abd_transactions;
CREATE EXTERNAL TABLE abd_transactions(
    sales_id                            string,
    patient_id                          string,
    patient_first_nam                   string,
    patient_last_nam                    string,
    patient_dob                         string,
    patient_age_nbr                     string,
    patient_sex_cd                      string,
    patient_ship_addr_dsc               string,
    patient_ship_city_nam               string,
    patient_ship_state_cd               string,
    patient_ship_zip_cd                 string,
    doctor_first_nam                    string,
    doctor_last_nam                     string,
    doctor_natl_provider_id             string,
    practice_city_nam                   string,
    practice_state_cd                   string,
    practice_zip_cd                     string,
    ins1_group_nbr_cd                   string,
    ins1_proc_cntrl_cd                  string,
    ins2_group_nbr_cd                   string,
    ins2_proc_cntrl_cd                  string,
    hvJoinKey                           string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {txn_input_path}
;

DROP TABLE IF EXISTS abd_additional_data;
CREATE EXTERNAL TABLE abd_additional_data(
    sales_cd                            string,
    is_active_flg                       string,
    ticket_dt                           string,
    rx_nbr                              string,
    fill_nbr                            string
    year_nbr                            string,
    mnth_nbr                            string,
    abd_location_id                     string,
    abd_location_nam                    string,
    cash_or_funded_cd                   string,
    sterile_or_non_cd                   string,
    fill_dt                             string,
    fill_qty                            string,
    days_supply_qty                     string,
    refill_remain_qty                   string,
    is_new_rx_flg                       string,
    is_compound_flg                     string,
    drug_dsc                            string,
    national_drug_cd                    string,
    disease_state_rpt_cd                string,
    sale_awp_amt                        string,
    ins_ref_nam                         string,
    ins1_plan_type_cd                   string,
    ins_ref_bin_nbr                     string,
    ins2_nam                            string,
    ins2_plan_type_cd                   string,
    ins2_bin_nbr_cd                     string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION {add_input_path}
;
