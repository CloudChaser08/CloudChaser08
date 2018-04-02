SELECT
    p.claimid                                                               AS claim_id,
    p.hvid                                                                  AS hvid,
    CASE
        WHEN t.patient_gender_code = '1' THEN 'M'
        WHEN t.patient_gender_code = '2' THEN 'F'
        ELSE 'U'
    END                                                                     AS patient_gender,
    p.age                                                                   AS patient_age,
    p.yearOfBirth                                                           AS patient_year_of_birth,
    SUBSTR(TRIM(p.threeDigitZip), 1, 3)                                     AS patient_zip3,
    TRIM(UPPER(COALESCE(p.state, '')))                                      AS patient_state,
    t.date_filled                                                           AS date_service,
    t.date_written                                                          AS date_written,
    TRIM(UPPER(t.claim_indicator))                                          AS response_code_vendor,
    TRIM(UPPER(t.diagnosis_code))                                           AS diagnosis_code,
    CASE
        WHEN t.diagnosis_code IS NOT NULL THEN '01'
        ELSE NULL
    END                                                                     AS diagnosis_code_qual,
    t.dispensed_ndc_number                                                  AS ndc_code,
    t.prescription_number_filler                                            AS rx_number,
    CASE
        WHEN t.prescription_number IS NOT NULL THEN '1'
        ELSE NULL
    END                                                                     AS rx_number_qual,
    t.bank_identification_number                                            AS bin_number,
    t.processor_control_number                                              AS processor_control_number,
    t.new_refill_counter                                                    AS fill_number,
    t.number_of_refills_authorized                                          AS refill_auth_amount,
    CONCAT(LEFT(t.quantity_dispensed, LENGTH(t.quantity_dispensed)-3),
           '.',
           RIGHT(t.quantity_dispensed, 3)
    )                                                                       AS dispensed_quantity,
    t.unit_of_measure                                                       AS unit_of_measure,
    t.days_supply                                                           AS days_supply,
    t.pharmacy_npi                                                          AS pharmacy_npi,
    t.plan_code                                                             AS payer_plan_id,
    CASE
        WHEN t.payment_type = '1' THEN 'Cash'
        WHEN t.payment_type = '2' THEN 'Medicaid'
        WHEN t.payment_type = '3' THEN 'Third Party'
        ELSE NULL
    END                                                                     AS payer_type,
    t.compound_code                                                         AS compound_code,
    t.dispensed_as_written                                                  AS dispensed_as_written,
    CASE
        WHEN t.origin_of_rx = '1' THEN 'WRITTEN'
        WHEN t.origin_of_rx = '2' THEN 'PHONE'
        WHEN t.origin_of_rx = '3' THEN 'ESCRIPT'
        WHEN t.origin_of_rx = '4' THEN 'FAX'
        ELSE NULL
    END                                                                     AS prescription_origin,
    t.prescribed_ndc_number                                                 AS orig_prescribed_product_service_code,
    CASE
        WHEN t.prescribed_ndc_number IS NOT NULL THEN '03'
        ELSE NULL
    END                                                                     AS orig_prescribed_product_service_code_qual,
    t.level_of_service                                                      AS level_of_service,
    t.prescriber_npi                                                        AS prov_prescribing_npi,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.state_liscense_number_for_prescriber
    END                                                                     AS prov_prescribing_state_license,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.prescriber_last_name    
    END                                                                     AS prov_prescribing_name_1,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.prescriber_first_name
    END                                                                     AS prov_prescribing_name_2,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.prescriber_city
    END                                                                     AS prov_prescribing_city,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.prescriber_state
    END                                                                     AS prov_prescribing_state,
    CASE
        WHEN t.level_of_service = '6' THEN NULL
        ELSE t.prescriber_zip_code
    END                                                                     AS prov_prescribing_zip,
    t.coordination_of_benefits_counter                                      AS cob_count,
    CONCAT(
        LEFT(t.copay_coinsurance_amount, LENGTH(t.copay_coinsurance_amount)-2),
        '.',
        RIGHT(t.copay_coinsurance_amount, 2)
    )                                                                       AS copay_coinsurance,
    t.basis_of_ingredient_cost_submitted                                    AS basis_of_cost_determination,
    CONCAT(
        LEFT(t.ingredient_cost, LENGTH(t.ingredient_cost)-2),
        '.',
        RIGHT(t.ingredient_cost, 2)
    )                                                                       AS submitted_ingredient_cost,
    CONCAT(
        LEFT(t.dispensing_fee, LENGTH(t.dispensing_fee)-2),
        '.',
        RIGHT(t.dispensing_fee, 2)
    )                                                                       AS submitted_dispensing_fee,
    t.basis_of_ingredient_cost_reimbursed                                   AS basis_of_reimbursement_determination,
    CONCAT(
        LEFT(t.ingredient_cost_paid, LENGTH(t.ingredient_cost_paid)-2),
        '.',
        RIGHT(t.ingredient_cost_paid, 2)
    )                                                                       AS paid_ingredient_cost,
    CONCAT(
        LEFT(t.dispensing_fee_paid, LENGTH(t.dispensing_fee_paid)-2),
        '.',
        RIGHT(t.dispensing_fee_paid, 2)
    )                                                                       AS paid_dispensing_fee,
    CONCAT(
        LEFT(t.reimbursed_amount, LENGTH(t.reimbursed_amount)-2),
        '.',
        RIGHT(t.reimbursed_amount, 2)
    )                                                                       AS paid_gross_due,
    CONCAT(
        LEFT(t.total_amount_paid_by_patient, LENGTH(t.total_amount_paid_by_patient)-2),
        '.',
        RIGHT(t.total_amount_paid_by_patient, 2)
    )                                                                       AS paid_patient_pay,
    t.indicator_for_coupon_type                                             AS coupon_type,
    t.coupon_id                                                             AS coupon_number,
    CONCAT(
        LEFT(t.coupon_face_value, LENGTH(t.coupon_face_value)-2),
        '.',
        RIGHT(t.coupon_face_value, 2)
    )                                                                       AS coupon_value,
    t.pharmacy_ncpdp_number                                                 AS pharmacy_other_id,
    CASE
        WHEN t.pharmacy_ncpdp_number IS NOT NULL THEN '07',
        ELSE NULL
    END                                                                     AS pharmacy_other_qual,
    t.pharmacy_zip_code                                                     AS pharmacy_postal_code,
    CASE
        WHEN t.claim_indicator = 'R' THEN 'Reversal'
        ELSE NULL
    END                                                                     AS logical_delete_reason
FROM pdx_transactions t
LEFT OUTER JOIN matching_payload p ON t.hvJoinKey = p.hvJoinKey
