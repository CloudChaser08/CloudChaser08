INSERT INTO pharmacyclaims_common_model
SELECT
    NULL,                                     -- record_id
    NULL,                                     -- claim_id
    mp.hvid,                                  -- hvid
    NULL,                                     -- created
    3,                                        -- model_version
    NULL,                                     -- data_set
    NULL,                                     -- data_feed
    NULL,                                     -- data_vendor
    t.version_release_number,                 -- source_version
    mp.gender,                                -- patient_gender
    NULL,                                     -- patient_age
    mp.yearOfBirth,                           -- patient_year_of_birth
    mp.threeDigitZip,                         -- patient_zip3
    UPPER(mp.state),                          -- patient_state
    extract_date(
        t.date_of_service, '%Y-%m-%d %H:%M:%S.%f', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_service
    extract_date(
        t.date_prescription_written, '%Y-%m-%d %H:%M:%S.%f', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_written
    NULL,                                     -- year_of_injury
    NULL,                                     -- date_authorized
    NULL,                                     -- time_authorized
    t.transaction_code,                       -- transaction_code_std
    NULL,                                     -- transaction_code_vendor
    t.transaction_response_status,            -- response_code_std
    NULL,                                     -- response_code_vendor
    NULL,                                     -- reject_reason_code_1
    NULL,                                     -- reject_reason_code_2
    NULL,                                     -- reject_reason_code_3
    NULL,                                     -- reject_reason_code_4
    NULL,                                     -- reject_reason_code_5
    NULL,                                     -- diagnosis_code
    t.diagnosis_code_qualifier,               -- diagnosis_code_qual
    NULL,                                     -- procedure_code
    NULL,                                     -- procedure_code_qual
    CASE
        WHEN t.product_code_qualifier IN ('9', 'N') THEN t.product_code
        ELSE NULL
    END,                                      -- ndc_code
    CASE
        WHEN t.product_code_qualifier NOT IN ('9', 'N') THEN t.product_code
        ELSE NULL
    END,                                      -- product_service_id
    CASE
        WHEN t.product_code_qualifier NOT IN ('9', 'N') THEN t.product_code_qualifier
        ELSE NULL
    END,                                      -- product_service_id_qual
    t.prescription_service_reference_number,  -- rx_number
    NULL,                                     -- rx_number_qual
    t.bin_number,                             -- bin_number
    t.processor_control_number,               -- processor_control_number
    t.fill_number,                            -- fill_number
    t.number_of_refills_authorized,           -- refill_auth_amount
    t.quantity_dispensed,                     -- dispensed_quantity
    NULL,                                     -- unit_of_measure
    t.days_supply,                            -- days_supply
    CASE
        WHEN t.servicer_provider_id_qual = '01' THEN t.service_provider_id ELSE NULL
    END,                                      -- pharmacy_npi
    NULL,                                     -- prov_dispensing_npi
    NULL,                                     -- payer_id
    NULL,                                     -- payer_id_qual
    NULL,                                     -- payer_name
    NULL,                                     -- payer_parent_name
    NULL,                                     -- payer_org_name
    t.plan_id,                                -- payer_plan_id
    t.plan_name,                              -- payer_plan_name
    NULL,                                     -- payer_type
    t.compound_code,                          -- compound_code
    t.unit_dose_indicator,                    -- unit_dose_indicator
    t.dispense_as_written_code,               -- dispensed_as_written
    NULL,                                     -- prescription_origin
    t.submission_clarification_code,          -- submission_clarification
    NULL,                                     -- orig_prescribed_product_service_code
    NULL,                                     -- orig_prescribed_product_service_code_qual
    NULL,                                     -- orig_prescribed_quantity
    t.prior_authorization_type_code,          -- prior_auth_type_code
    t.level_of_service,                       -- level_of_service
    NULL,                                     -- reason_for_service
    NULL,                                     -- professional_service_code
    NULL,                                     -- result_of_service_code
    NULL,                                     -- prov_prescribing_npi
    NULL,                                     -- prov_primary_care_npi
    t.coordination_of_benefits_count,         -- cob_count
    t.submitted_usual_and_customary_amount,   -- usual_and_customary_charge
    NULL,                                     -- product_selection_attributed
    t.response_other_payer_amount_recognized, -- other_payer_recognized
    NULL,                                     -- periodic_deductible_applied
    NULL,                                     -- periodic_benefit_exceed
    NULL,                                     -- accumulated_deductible
    NULL,                                     -- remaining_deductible
    NULL,                                     -- remaining_benefit
    t.response_patient_copay,                 -- copay_coinsurance
    NULL,                                     -- basis_of_cost_determination
    t.submitted_ingredient_cost,              -- submitted_ingredient_cost
    t.submitted_dispensing_fee,               -- submitted_dispensing_fee
    NULL,                                     -- submitted_incentive
    t.submitted_gross_due_amount,             -- submitted_gross_due
    NULL,                                     -- submitted_professional_service_fee
    t.submitted_patient_paid_amount,          -- submitted_patient_pay
    NULL,                                     -- submitted_other_claimed_qual
    NULL,                                     -- submitted_other_claimed
    t.basis_of_reimbursement_determination,   -- basis_of_reimbursement_determination
    t.response_ingredient_cost_paid,          -- paid_ingredient_cost
    t.response_dispensing_fee_paid,           -- paid_dispensing_fee
    t.response_incentive_amount_paid,         -- paid_incentive
    t.response_amount_paid,                   -- paid_gross_due
    NULL,                                     -- paid_professional_service_fee
    t.response_patient_pay_amount,            -- paid_patient_pay
    NULL,                                     -- paid_other_claimed_qual
    t.response_other_amount_paid,             -- paid_other_claimed
    t.submitted_tax_exempt_indicator,         -- tax_exempt_indicator
    NULL,                                     -- coupon_type
    NULL,                                     -- coupon_number
    NULL,                                     -- coupon_value
    CASE
        WHEN t.pharmacy_ncpdp IS NOT NULL THEN CONCAT(REPEAT('0', 7-LENGTH(t.pharmacy_ncpdp)), t.pharmacy_ncpdp)
        WHEN t.servicer_provider_id_qual IN ('7', '07') THEN CONCAT(REPEAT('0', 7-LENGTH(t.service_provider_id)), t.service_provider_id)
        WHEN t.servicer_provider_id_qual != '01' THEN t.service_provider_id
        ELSE NULL
    END,                                      -- pharmacy_other_id
    CASE
        WHEN t.pharmacy_ncpdp IS NOT NULL THEN '07'
        WHEN t.servicer_provider_id_qual != '01' THEN t.servicer_provider_id_qual
        ELSE NULL
    END,                                      -- pharmacy_other_qual
    NULL,                                     -- pharmacy_postal_code
    t.provider_id,                            -- prov_dispensing_id
    t.provider_id_qualifier,                  -- prov_dispensing_qual
    t.prescriber_id,                          -- prov_prescribing_id
    t.prescriber_id_qualifier,                -- prov_prescribing_qual
    NULL,                                     -- prov_primary_care_id
    NULL,                                     -- prov_primary_care_qual
    t.other_payor_coverage_type,              -- other_payer_coverage_type
    t.other_payer_id,                         -- other_payer_coverage_id
    t.other_payer_id_qualifier,               -- other_payer_coverage_qual
    NULL,                                     -- other_payer_date
    t.other_coverage_code,                    -- other_payer_coverage_code
    CASE
        WHEN t.transaction_response_status = 'R' THEN 'Claim Rejected'
        WHEN t.transaction_code = 'B2' THEN 'Reversal'
        ELSE NULL
    END                                       -- logical_delete_reason
FROM transactions t
    LEFT JOIN matching_payload mp ON t.unique_patient_id = mp.personId
;
