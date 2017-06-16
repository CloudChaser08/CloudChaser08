DROP TABLE IF EXISTS transactions;
CREATE EXTERNAL TABLE transactions (
        claim_id                                   string,
        patient_first_name                         string,
        patient_last_name                          string,
        patient_gender                             string,
        patient_dob                                string,
        patient_age                                string,
        patient_address                            string,
        patient_city                               string,
        patient_state                              string,
        patient_zip                                string,
        filldate                                   string,
        writtendate                                string,
        date_injury                                string,
        date_authorized                            string,
        time_authorized                            string,
        transaction_code_std                       string,
        transaction_code_vendor                    string,
        response_code_std                          string,
        response_code_vendor                       string,
        reject_reason_code_1                       string,
        reject_reason_code_2                       string,
        reject_reason_code_3                       string,
        reject_reason_code_4                       string,
        reject_reason_code_5                       string,
        diagnosiscode                              string,
        qualifiertypetext                          string,
        procedure_code                             string,
        procedure_code_qual                        string,
        ndccode                                    string,
        product_service_id                         string,
        product_service_id_qual                    string,
        prescriptionid                             string,
        rx_number_qual                             string,
        binnbr                                     string,
        pcnnbr                                     string,
        refillnbr                                  string,
        writtenrefillcnt                           string,
        fillquantity                               string,
        unit_of_measure                            string,
        dayssupplydispensedcnt                     string,
        npinbr                                     string,
        prov_dispensing_npi                        string,
        payer_id                                   string,
        payer_id_qual                              string,
        billname                                   string,
        billname2                                  string,
        payer_parent_name                          string,
        payer_org_name                             string,
        payer_plan_id                              string,
        planname                                   string,
        planname2                                  string,
        payer_type                                 string,
        compound_code                              string,
        unit_dose_indicator                        string,
        dispenseaswrittenind                       string,
        origintext                                 string,
        submission_clarification                   string,
        orig_prescribed_product_service_code       string,
        orig_prescribed_product_service_code_qual  string,
        writtenqty                                 string,
        prior_auth_type_code                       string,
        level_of_service                           string,
        reason_for_service                         string,
        professional_service_code                  string,
        result_of_service_code                     string,
        prov_primary_care_npi                      string,
        prov_primary_care_npi2                     string,
        cob_count                                  string,
        usualandcustomary                          string,
        taxamt                                     string,
        product_selection_attributed               string,
        other_payer_recognized                     string,
        periodic_deductible_applied                string,
        periodic_benefit_exceed                    string,
        accumulated_deductible                     string,
        remaining_deductible                       string,
        remaining_benefit                          string,
        copay_coinsurance                          string,
        basis_of_cost_determination                string,
        costamt                                    string,
        dispensingfeeamt                           string,
        discountamt                                string,
        submitted_gross_due                        string,
        submitted_professional_service_fee         string,
        submitted_flat_sales_tax                   string,
        submitted_percent_sales_tax_basis          string,
        submitted_percent_sales_tax_rate           string,
        submitted_percent_sales_tax_amount         string,
        copayamt                                   string,
        submitted_other_claimed_qual               string,
        submitted_other_claimed                    string,
        basis_of_reimbursement_determination       string,
        payoramt                                   string,
        paid_dispensing_fee                        string,
        paid_incentive                             string,
        paid_gross_due                             string,
        paid_professional_service_fee              string,
        paid_flat_sales_tax                        string,
        paid_percent_sales_tax_basis               string,
        paid_percent_sales_tax_rate                string,
        paid_percent_sales_tax                     string,
        paid_patient_pay                           string,
        paid_other_claimed_qual                    string,
        paid_other_claimed                         string,
        tax_exempt_indicator                       string,
        coupon_type                                string,
        coupon_number                              string,
        coupon_value                               string,
        ncpdpproviderid                            string,
        providertype                               string,
        zipcode                                    string,
        prov_dispensing_id                         string,
        prov_dispensing_qual                       string,
        prov_prescribing_npi                       string,
        prov_prescribing_npi2                      string,
        prov_prescribing_qual                      string,
        prov_primary_care_id                       string,
        prov_primary_care_qual                     string,
        payorprioritynbr                           string,
        other_payer_coverage_id                    string,
        other_payer_coverage_qual                  string,
        other_payer_date                           string,
        other_payer_coverage_code                  string,
        logical_delete_reason                      string,
        filler                                     string,
        hvJoinKey                                  string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
