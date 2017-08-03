INSERT INTO pharmacyclaims_common_model
SELECT
    NULL,                                     -- record_id
    pharmacy_claim_id,                        -- claim_id
    mp.hvid,                                  -- hvid
    NULL,                                     -- created
    3,                                        -- model_version
    NULL,                                     -- data_set
    NULL,                                     -- data_feed
    NULL,                                     -- data_vendor
    NULL,                                     -- source_version
    mp.gender,                                -- patient_gender
    NULL,                                     -- patient_age
    mp.yearOfBirth,                           -- patient_year_of_birth
    mp.threeDigitZip,                         -- patient_zip3
    UPPER(t.patient_state),                   -- patient_state
    extract_date(
        t.date_of_service, '%Y-%m-%d %H:%M:%S.%f', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_service
    extract_date(
        t.date_prescription_written, '%Y-%m-%d %H:%M:%S.%f', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_written
    NULL,                                     -- year_of_injury
    NULL,                                     -- date_authorized
    NULL,                                     -- time_authorized
    NULL,                                     -- transaction_code_std
    CASE WHEN LOWER(t.transaction_code) = 'd' THEN 'D'
        ELSE NULL
    END,                                      -- transaction_code_vendor
    NULL,                                     -- response_code_std
    CASE WHEN LOWER(t.transaction_code) = 'r' THEN 'R'
        ELSE NULL
    END,                                      -- response_code_vendor
    NULL,                                     -- reject_reason_code_1
    NULL,                                     -- reject_reason_code_2
    NULL,                                     -- reject_reason_code_3
    NULL,                                     -- reject_reason_code_4
    NULL,                                     -- reject_reason_code_5
    t.diagnosis_code,                         -- diagnosis_code
    t.diagnosis_code_qualifier,               -- diagnosis_code_qual
    CASE WHEN t.product_service_id_qualifier IN ('7', '8', '9', '07', '08', '09')
        THEN t.product_service_id
        ELSE NULL
    END,                                      -- procedure_code
    CASE WHEN t.product_service_id_qualifier IN ('7', '8', '9', '07', '08', '09')
        THEN t.product_service_id_qualifier
        ELSE NULL
    END,                                      -- procedure_code_qual
    CASE WHEN t.product_service_id_qualifier IN ('3', '03')
        THEN t.product_service_id
        ELSE NULL
    END,                                      -- ndc_code
    CASE WHEN t.product_service_id_qualifier NOT IN ('3', '7', '8', '9', '03', '07', '08', '09')
        THEN t.product_service_id
        ELSE NULL
    END,                                      -- product_service_id
    CASE WHEN t.product_service_id_qualifier NOT IN ('3', '7', '8', '9', '03', '07', '08', '09')
        THEN t.product_service_id_qualifier
        ELSE NULL
    END,                                      -- product_service_id_qual
    mp.rxnumber,                              -- rx_number
    t.prescription_service_reference_number_qualifier,
                                              -- rx_number_qual
    NULL,                                     -- bin_number
    t.processor_control_number,               -- processor_control_number
    t.fill_number,                            -- fill_number
    t.number_of_refills_authorized,           -- refill_auth_amount
    t.quantity_dispensed,                     -- dispensed_quantity
    t.unit_of_measure,                        -- unit_of_measure
    t.days_supply,                            -- days_supply
    CASE
        WHEN (t.service_provider_id_qual IN ('1', '01')) OR
            (t.service_provider_id_qual IN ('5', '05') 
                AND regexp_replace(t.service_provider_id, '^[0-9]{10}$', '') = '')
            THEN t.service_provider_id
        ELSE NULL
    END,                                      -- pharmacy_npi
    NULL,                                     -- prov_dispensing_npi
    NULL,                                     -- payer_id
    NULL,                                     -- payer_id_qual
    NULL,                                     -- payer_name
    NULL,                                     -- payer_parent_name
    NULL,                                     -- payer_org_name
    NULL,                                     -- payer_plan_id
    NULL,                                     -- payer_plan_name
    NULL,                                     -- payer_type
    t.compound_code,                          -- compound_code
    t.unit_dose_indicator,                    -- unit_dose_indicator
    t.dispense_as_written_code,               -- dispensed_as_written
    NULL,                                     -- prescription_origin
    NULL,                                     -- submission_clarification
    NULL,                                     -- orig_prescribed_product_service_code
    NULL,                                     -- orig_prescribed_product_service_code_qual
    NULL,                                     -- orig_prescribed_quantity
    NULL,                                     -- prior_auth_type_code
    t.level_of_service,                       -- level_of_service
    NULL,                                     -- reason_for_service
    NULL,                                     -- professional_service_code
    NULL,                                     -- result_of_service_code
    CASE
        WHEN (t.prescriber_id_qualifier_qual IN ('1', '01')) OR
            (t.prescriber_id_qualifier_qual IN ('5', '05') 
                AND regexp_replace(t.prescriber_id_qualifier, '^[0-9]{10}$', '') = '')
            THEN t.prescriber_id
        ELSE NULL
    END,                                      -- prov_prescribing_npi
    NULL,                                     -- prov_primary_care_npi
    NULL,                                     -- cob_count
    NULL,                                     -- usual_and_customary_charge
    NULL,                                     -- product_selection_attributed
    NULL,                                     -- other_payer_recognized
    NULL,                                     -- periodic_deductible_applied
    NULL,                                     -- periodic_benefit_exceed
    NULL,                                     -- accumulated_deductible
    NULL,                                     -- remaining_deductible
    NULL,                                     -- remaining_benefit
    NULL,                                     -- copay_coinsurance
    NULL,                                     -- basis_of_cost_determination
    NULL,                                     -- submitted_ingredient_cost
    NULL,                                     -- submitted_dispensing_fee
    NULL,                                     -- submitted_incentive
    NULL,                                     -- submitted_gross_due
    NULL,                                     -- submitted_professional_service_fee
    NULL,                                     -- submitted_patient_pay
    NULL,                                     -- submitted_other_claimed_qual
    NULL,                                     -- submitted_other_claimed
    NULL,                                     -- basis_of_reimbursement_determination
    NULL,                                     -- paid_ingredient_cost
    NULL,                                     -- paid_dispensing_fee
    NULL,                                     -- paid_incentive
    NULL,                                     -- paid_gross_due
    NULL,                                     -- paid_professional_service_fee
    NULL,                                     -- paid_patient_pay
    NULL,                                     -- paid_other_claimed_qual
    NULL,                                     -- paid_other_claimed
    NULL,                                     -- tax_exempt_indicator
    NULL,                                     -- coupon_type
    NULL,                                     -- coupon_number
    NULL,                                     -- coupon_value
    CASE
        WHEN (t.service_provider_id_qual NOT IN ('1', '01')) AND
            (t.service_provider_id_qual NOT IN ('5', '05') 
                OR regexp_replace(t.service_provider_id, '^[0-9]{10}$', '') != '')
            THEN t.service_provider_id
        ELSE NULL
    END,                                      -- pharmacy_other_id
    CASE
        WHEN (t.service_provider_id_qual NOT IN ('1', '01')) AND
            (t.service_provider_id_qual NOT IN ('5', '05') 
                OR regexp_replace(t.service_provider_id, '^[0-9]{10}$', '') != '')
            THEN t.service_provider_id_qualifier
        ELSE NULL
    END,                                      -- pharmacy_other_qual
    NULL,                                     -- pharmacy_postal_code
    NULL,                                     -- prov_dispensing_id
    NULL,                                     -- prov_dispensing_qual
    CASE
        WHEN (t.prescriber_id_qualifier_qual NOT IN ('1', '01')) AND
            (t.prescriber_id_qualifier_qual NOT IN ('5', '05') 
                OR regexp_replace(t.prescriber_id_qualifier, '^[0-9]{10}$', '') != '')
            THEN t.prescriber_id
        ELSE NULL
    END,                                      -- prov_prescribing_id
    CASE
        WHEN (t.prescriber_id_qualifier_qual NOT IN ('1', '01')) AND
            (t.prescriber_id_qualifier_qual NOT IN ('5', '05') 
                OR regexp_replace(t.prescriber_id_qualifier, '^[0-9]{10}$', '') != '')
            THEN t.prescriber_id_qual
        ELSE NULL
    END,                                      -- prov_prescribing_qual
    NULL,                                     -- prov_primary_care_id
    NULL,                                     -- prov_primary_care_qual
    NULL,                                     -- other_payer_coverage_type
    NULL,                                     -- other_payer_coverage_id
    NULL,                                     -- other_payer_coverage_qual
    NULL,                                     -- other_payer_date
    NULL,                                     -- other_payer_coverage_code
    NULL                                      -- logical_delete_reason
FROM transactions t
    LEFT JOIN matching_payload mp ON t.hv_join_key = mp.hvJoinKey
;
