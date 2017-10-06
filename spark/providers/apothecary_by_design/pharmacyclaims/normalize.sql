INSERT INTO pharmacyclaims_common_model
SELECT
    NULL,                                       -- record_id
    ad.sales_cd,                                -- claim_id
    pay.hvid,                                   -- hvid
    NULL,                                       -- created
    NULL,                                       -- model_version
    NULL,                                       -- data_set
    NULL,                                       -- data_feed
    NULL,                                       -- data_vendor
    NULL,                                       -- source_version
    COALESCE(pay.gender, txn.patient_sex_cd),   -- patient_gender
    COALESCE(pay.age, txn.patient_age_nbr),     -- patient_age
    COALESCE(pay.yearOfBirth, txn.patient_dob), -- patient_year_of_birth
    COALESCE(pay.threeDigitZip,
        SUBSTR(txn.patient_ship_zip_cd, 1, 3)), -- patient_zip3
    COALESCE(pay.state, 
        txn.patient_ship_state_cd),             -- patient_state
    extract_date(
        COALESCE(ad.fill_dt, ad.ticket_dt),
        '%Y%m%d',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
    ),                                          -- date_service
    NULL,                                       -- date_written
    NULL,                                       -- year_of_injury
    NULL,                                       -- date_authorized
    NULL,                                       -- time_authorized
    NULL,                                       -- transaction_code_std
    NULL,                                       -- transaction_code_vendor
    NULL,                                       -- response_code_std
    NULL,                                       -- response_code_vendor
    NULL,                                       -- reject_reason_code_1
    NULL,                                       -- reject_reason_code_2
    NULL,                                       -- reject_reason_code_3
    NULL,                                       -- reject_reason_code_4
    NULL,                                       -- reject_reason_code_5
    NULL,                                       -- diagnosis_code
    NULL,                                       -- diagnosis_code_qual
    NULL,                                       -- procedure_code
    NULL,                                       -- procedure_code_qual
    clean_up_numeric_code(
            ad.national_drug_cd),               -- ndc_code
    NULL,                                       -- product_service_id
    NULL,                                       -- product_service_id_qual
    ad.rx_nbr,                                  -- rx_number
    NULL,                                       -- rx_number_qual
    ad.ins_ref_bin_nbr,                         -- bin_number
    CASE
        WHEN UPPER(txn.ins1_proc_cntrl_cd) = 'UNKOWN' THEN NULL
        ELSE txn.ins1_proc_cntrl_cd
    END,                                        -- processor_control_number
    ad.fill_nbr,                                -- fill_number
    ad.refill_remain_qty,                       -- refill_auth_amount
    ad.fill_qty,                                -- dispensed_quantity
    NULL,                                       -- unit_of_measure
    ad.days_supply_qty,                         -- days_supply
    NULL,                                       -- pharmacy_npi
    NULL,                                       -- prov_dispensing_npi
    NULL,                                       -- payer_id
    NULL,                                       -- payer_id_qual
    TRIM(UPPER(ad.ins_ref_nam)),                -- payer_name
    NULL,                                       -- payer_parent_name
    NULL,                                       -- payer_org_name
    NULL,                                       -- payer_plan_id
    NULL,                                       -- payer_plan_name
    TRIM(UPPER(ad.ins1_plan_type_cd)),          -- payer_type
    CASE
        WHEN ad.is_compound_flg = 'True' THEN '2'
        WHEN ad.is_compound_flg = 'False' THEN '1'
        ELSE '0'
    END,                                        -- compound_code
    NULL,                                       -- unit_dose_indicator
    NULL,                                       -- dispensed_as_written
    NULL,                                       -- prescription_origin
    NULL,                                       -- submission_clarification
    NULL,                                       -- orig_prescribed_product_service_code
    NULL,                                       -- orig_prescribed_product_service_code_qual
    NULL,                                       -- orig_prescribed_quantity
    NULL,                                       -- prior_auth_type_code
    NULL,                                       -- level_of_service
    NULL,                                       -- reason_for_service
    NULL,                                       -- professional_service_code
    NULL,                                       -- result_of_service_code
    txn.doctor_natl_provider_id,                -- prov_prescribing_npi
    NULL,                                       -- prov_primary_care_npi
    NULL,                                       -- cob_count
    NULL,                                       -- usual_and_customary_charge
    NULL,                                       -- product_selection_attributed
    NULL,                                       -- other_payer_recognized
    NULL,                                       -- periodic_deductible_applied
    NULL,                                       -- periodic_benefit_exceed
    NULL,                                       -- accumulated_deductible
    NULL,                                       -- remaining_deductible
    NULL,                                       -- remaining_benefit
    NULL,                                       -- copay_coinsurance
    NULL,                                       -- basis_of_cost_determination
    NULL,                                       -- submitted_ingredient_cost
    NULL,                                       -- submitted_dispensing_fee
    NULL,                                       -- submitted_incentive
    NULL,                                       -- submitted_gross_due
    NULL,                                       -- submitted_professional_service_fee
    NULL,                                       -- submitted_patient_pay
    NULL,                                       -- submitted_other_claimed_qual
    NULL,                                       -- submitted_other_claimed
    NULL,                                       -- basis_of_reimbursement_determination
    NULL,                                       -- paid_ingredient_cost
    NULL,                                       -- paid_dispensing_fee
    NULL,                                       -- paid_incentive
    NULL,                                       -- paid_gross_due
    NULL,                                       -- paid_professional_service_fee
    NULL,                                       -- paid_patient_pay
    NULL,                                       -- paid_other_claimed_qual
    NULL,                                       -- paid_other_claimed
    NULL,                                       -- tax_exempt_indicator
    NULL,                                       -- coupon_type
    NULL,                                       -- coupon_number
    NULL,                                       -- coupon_value
    ad.abd_location_id,                         -- pharmacy_other_id
    CASE
        WHEN ad.abd_location_id = NULL THEN NULL
        ELSE 'VENDOR'
    END,                                        -- pharmacy_other_qual
    NULL,                                       -- pharmacy_postal_code
    NULL,                                       -- prov_dispensing_id
    NULL,                                       -- prov_dispensing_qual
    NULL,                                       -- prov_prescribing_id
    NULL,                                       -- prov_prescribing_qual
    NULL,                                       -- prov_primary_care_id
    NULL,                                       -- prov_primary_care_qual
    NULL,                                       -- other_payer_coverage_type
    NULL,                                       -- other_payer_coverage_id
    NULL,                                       -- other_payer_coverage_qual
    NULL,                                       -- other_payer_date
    NULL,                                       -- other_payer_coverage_code
    NULL                                        -- logical_delete_reason
FROM abd_additional_data ad
    LEFT OUTER JOIN abd_transactions txn ON txn.sales_id = ad.sales_cd
    LEFT OUTER JOIN matching_payload pay ON txn.hvjoinkey = pay.hvJoinKey
WHERE
    is_active_flg = 'True'
    AND ad.sales_cd <> 'sales_cd'
;
