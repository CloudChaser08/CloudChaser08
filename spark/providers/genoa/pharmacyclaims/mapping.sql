SELECT
    sales_key                                   AS claim_id,
    hvid                                        AS hvid,
    input_file_name                             AS data_set,
    CASE WHEN UPPER(gender) = 'M' OR UPPER(patient_gender) = 'M' THEN 'M'
        WHEN UPPER(gender) = 'F' OR UPPER(patient_gender) = 'F' THEN 'F'
        ELSE 'U' END                            AS patient_gender,
    yearOfBirth                                 AS patient_year_of_birth,
    threeDigitZip                               AS patient_zip3,
    UPPER(z3.state)                             AS patient_state,
    extract_date(date_of_service, '%Y-%m-%d')   AS date_service,
    transaction_code                            AS transaction_code_std,
    response_code                               AS response_code_std,
    CASE WHEN product_service_id_qualifier IN ('7','8','9','07','08','09')
        THEN product_service_id END             AS procedure_code,
    CASE WHEN product_service_id_qualifier IN ('7','8','9','07','08','09')
        THEN product_service_id_qualifier END   AS procedure_code_qual,
    CASE WHEN product_service_id_qualifier IN ('3','03')
        THEN product_service_id END             AS ndc_code,
    CASE WHEN product_service_id_qualifier NOT IN ('3','7','8','9','03','07','08','09')
        THEN product_service_id END             AS product_service_id,
    CASE WHEN product_service_id_qualifier NOT IN ('3','7','8','9','03','07','08','09')
        THEN product_service_id_qualifier END   AS product_service_id_qual,
    prescription_service_reference_number       AS rx_number,
    prescription_service_reference_number_qualifier
                                                AS rx_number_qual,
    bin_number                                  AS bin_number,
    processor_control_number                    AS processor_control_number,
    fill_number                                 AS fill_number,
    number_of_refills_authorized                AS refill_auth_amount,
    quantity_dispensed                          AS dispensed_quantity,
    unit_of_measure                             AS unit_of_measure,
    days_supply                                 AS days_supply,
    CASE WHEN service_provider_id_qualifier IN ('1', '01')
        THEN service_provider_id END            AS pharmacy_npi,
    payer_id                                    AS payer_id,
    payer_id_qualifier                          AS payer_id_qual,
    plan_identification                         AS payer_plan_id,
    plan_name                                   AS payer_plan_name,
    compound_code                               AS compound_code,
    CASE WHEN prescriber_id_qualifier = 'P'
        THEN prescriber_id END                  AS prov_prescribing_npi,
    amount_of_copay_coinsurance                 AS copay_coinsurance,
    ingredient_cost_paid                        AS paid_ingredient_cost,
    dispensing_fee_paid                         AS paid_dispensing_fee,
    total_amount_paid                           AS paid_gross_due,
    CASE WHEN service_provider_id_qualifier NOT IN ('1', '01')
        THEN service_provider_id END            AS pharmacy_other_id,
    CASE WHEN service_provider_id_qualifier NOT IN ('1', '01')
        THEN service_provider_id_qualifier END  AS pharmacy_other_qual,
    pharmacy_location__postal_code_             AS pharmacy_postal_code,
    CASE WHEN prescriber_id_qualifier != 'P'
        THEN prescriber_id END                  AS prov_prescribing_id,
    CASE WHEN prescriber_id_qualifier != 'P'
        THEN prescriber_id_qualifier END        AS prov_prescribing_qual,
    CASE WHEN response_code = 'R' THEN 'Rejected'
        WHEN transaction_code = 'B2' THEN 'Reversal'
        END                                     AS logical_delete_reason
FROM genoa_rx_raw
    LEFT JOIN matching_payload ON hv_join_key = hvjoinkey
    LEFT JOIN zip3_to_state z3 ON threeDigitZip = zip3
