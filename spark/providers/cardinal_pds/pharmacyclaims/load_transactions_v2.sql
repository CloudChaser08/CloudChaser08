DROP TABLE IF EXISTS transactions;
CREATE EXTERNAL TABLE transactions (
        version_release_number                      string,
        date_of_service                             string,
        date_prescription_written                   string,
        transaction_received_date                   string,
        pharmacy_ncpdp                              string,
        servicer_provider_id_qual                   string,
        service_provider_id                         string,
        transaction_code                            string,
        transaction_response_status                 string,
        prescription_service_reference_number       string,
        product_code_qualifier                      string,
        product_code                                string,
        compound_code                               string,
        number_of_refills_authorized                string,
        fill_number                                 string,
        bin_number                                  string,
        processor_control_number                    string,
        group_id                                    string,
        cardholder_id                               string,
        person_code                                 string,
        patient_relationship_code                   string,
        patient_first_name                          string,
        patient_last_name                           string,
        date_of_birth                               string,
        patient_gender                              string,
        dispense_as_written_code                    string,
        prescriber_id_qualifier                     string,
        prescriber_id                               string,
        quantity_dispensed                          string,
        days_supply                                 string,
        submitted_patient_paid_amount               string,
        submitted_usual_and_customary_amount        string,
        submitted_gross_due_amount                  string,
        submitted_dispensing_fee                    string,
        submitted_ingredient_cost                   string,
        submitted_other_payer_amount_paid           string,
        submitted_flat_sales_tax_amount             string,
        submitted_percentage_sales_tax_amount       string,
        submitted_percentage_sales_tax_rate         string,
        submitted_percentage_sales_tax_basis        string,
        submitted_tax_exempt_indicator              string,
        response_patient_copay                      string,
        response_patient_pay_amount                 string,
        response_amount_paid                        string,
        response_dispensing_fee_paid                string,
        response_ingredient_cost_paid               string,
        response_other_payer_amount_recognized      string,
        response_flat_sales_tax_amount_paid         string,
        response_percentage_sales_tax_amount_paid   string,
        response_percentage_sales_tax_rate_paid     string,
        response_percentage_sales_tax_basis_paid    string,
        response_incentive_amount_paid              string,
        response_other_amount_paid                  string,
        basis_of_reimbursement_determination        string,
        coordination_of_benefits_count              string,
        other_coverage_code                         string,
        other_payer_id_qualifier                    string,
        other_payer_id                              string,
        other_payor_coverage_type                   string,
        other_payer_amount_paid_qualifier           string,
        eligibility_clarification_code              string,
        submission_clarification_code               string,
        dispensing_status                           string,
        level_of_service                            string,
        unit_dose_indicator                         string,
        prior_authorization_type_code               string,
        prior_authorization_number_submitted        string,
        provider_id_qualifier                       string,
        provider_id                                 string,
        approved_message_code                       string,
        patient_street_address                      string,
        patient_city_address                        string,
        patient_state_address                       string,
        patient_zip_postal_code                     string,
        patient_phone_number                        string,
        prescriber_last_name                        string,
        prescriber_phone_id                         string,
        diagnosis_code_qualifier                    string,
        plan_id                                     string,
        plan_id_code                                string,
        plan_name                                   string,
        patient_location_code                       string,
        unique_patient_id                           string,
        row_id                                      string,
        tenant_id                                   string,
        hvm_approved                                string,
        hv_join_key                                 string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = "|"
    )
    STORED AS TEXTFILE
    LOCATION {input_path}
    ;
