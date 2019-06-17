SELECT
    regexp_replace(t.row_id, '[{{}}]', '')    AS claim_id,
    t.unique_patient_id                       AS hvid,
    6                                         AS model_version,
    t.version_release_number                  AS source_version,
    COALESCE(t.patient_gender, mp.gender)     AS patient_gender,
    mp.yearOfBirth                            AS patient_year_of_birth,
    mp.threeDigitZip                          AS patient_zip3,
    UPPER(
        COALESCE(t.patient_state_address, mp.state)
    )                                         AS patient_state,
    extract_date(
        t.date_of_service, '%Y-%m-%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                     AS date_service,
    extract_date(
        t.date_prescription_written, '%Y-%m-%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                     AS date_written,
    t.transaction_code                        AS transaction_code_std,
    t.transaction_response_status             AS response_code_std,
    t.diagnosis_code_qualifier                AS diagnosis_code_qual,
    CASE
        WHEN t.product_code_qualifier IN ('9', 'N') THEN t.product_code
        ELSE NULL
    END                                       AS ndc_code,
    CASE
        WHEN t.product_code_qualifier NOT IN ('9', 'N') THEN t.product_code
        ELSE NULL
    END                                       AS product_service_id,
    CASE
        WHEN t.product_code_qualifier NOT IN ('9', 'N') THEN t.product_code_qualifier
        ELSE NULL
    END                                       AS product_service_id_qual,
    t.prescription_service_reference_number   AS rx_number,
    t.bin_number                              AS bin_number,
    t.processor_control_number                AS processor_control_number,
    t.fill_number                             AS fill_number,
    t.number_of_refills_authorized            AS refill_auth_amount,
    t.quantity_dispensed                      AS dispensed_quantity,
    t.days_supply                             AS days_supply,
    CASE
        WHEN t.servicer_provider_id_qual = '01' THEN t.service_provider_id ELSE NULL
    END                                       AS pharmacy_npi,
    CASE
        WHEN t.provider_id_qualifier = '1' THEN t.provider_id
        ELSE NULL
    END                                       AS prov_dispensing_npi,
    t.plan_id                                 AS payer_plan_id,
    t.plan_name                               AS payer_plan_name,
    t.compound_code                           AS compound_code,
    t.unit_dose_indicator                     AS unit_dose_indicator,
    t.dispense_as_written_code                AS dispensed_as_written,
    t.submission_clarification_code           AS submission_clarification,
    t.prior_authorization_type_code           AS prior_auth_type_code,
    t.level_of_service                        AS level_of_service,
    CASE
        WHEN t.prescriber_id_qualifier = '01' THEN t.prescriber_id
        ELSE NULL
    END                                       AS prov_prescribing_npi,
    t.coordination_of_benefits_count          AS cob_count,
    t.submitted_usual_and_customary_amount    AS usual_and_customary_charge,
    t.response_other_payer_amount_recognized  AS other_payer_recognized,
    t.response_patient_copay                  AS copay_coinsurance,
    t.submitted_ingredient_cost               AS submitted_ingredient_cost,
    t.submitted_dispensing_fee                AS submitted_dispensing_fee,
    t.submitted_gross_due_amount              AS submitted_gross_due,
    t.submitted_patient_paid_amount           AS submitted_patient_pay,
    t.basis_of_reimbursement_determination    AS basis_of_reimbursement_determination,
    t.response_ingredient_cost_paid           AS paid_ingredient_cost,
    t.response_dispensing_fee_paid            AS paid_dispensing_fee,
    t.response_incentive_amount_paid          AS paid_incentive,
    t.response_amount_paid                    AS paid_gross_due,
    t.response_patient_pay_amount             AS paid_patient_pay,
    t.response_other_amount_paid              AS paid_other_claimed,
    t.submitted_tax_exempt_indicator          AS tax_exempt_indicator,
    CASE
        WHEN t.pharmacy_ncpdp IS NOT NULL THEN CONCAT(REPEAT('0', 7-LENGTH(t.pharmacy_ncpdp)), t.pharmacy_ncpdp)
        WHEN t.servicer_provider_id_qual IN ('7', '07') THEN CONCAT(REPEAT('0', 7-LENGTH(t.service_provider_id)), t.service_provider_id)
        WHEN t.servicer_provider_id_qual != '01' THEN t.service_provider_id
        ELSE NULL
    END                                       AS pharmacy_other_id,
    CASE
        WHEN t.pharmacy_ncpdp IS NOT NULL THEN '07'
        WHEN t.servicer_provider_id_qual != '01' AND t.service_provider_id IS NOT NULL THEN t.servicer_provider_id_qual
        ELSE NULL
    END                                       AS pharmacy_other_qual,
    CASE
        WHEN t.provider_id_qualifier <> '1' THEN t.provider_id
        ELSE NULL
    END                                       AS prov_dispensing_id,
    CASE
        WHEN t.provider_id_qualifier <> '1' AND t.provider_id IS NOT NULL THEN t.provider_id_qualifier
        ELSE NULL
    END                                       AS prov_dispensing_qual,
    CASE
        WHEN t.prescriber_id_qualifier <> '01' THEN t.prescriber_id
        ELSE NULL
    END                                       AS prov_prescribing_id,
    CASE
        WHEN t.prescriber_id_qualifier <> '01' AND t.prescriber_id IS NOT NULL THEN t.prescriber_id_qualifier
        ELSE NULL
    END                                       AS prov_prescribing_qual,
    t.other_payor_coverage_type               AS other_payer_coverage_type,
    t.other_payer_id                          AS other_payer_coverage_id,
    t.other_payer_id_qualifier                AS other_payer_coverage_qual,
    t.other_coverage_code                     AS other_payer_coverage_code,
    CASE
        WHEN t.transaction_response_status = 'R' THEN 'Claim Rejected'
        WHEN t.transaction_code = 'B2' THEN 'Reversal'
        ELSE NULL
    END                                       AS logical_delete_reason,
    t.tenant_id                               AS tenant_id -- 05/30 Added at Cardinal's request
FROM transactions t
    LEFT JOIN matching_payload mp ON t.hv_join_key = mp.hvJoinKey
