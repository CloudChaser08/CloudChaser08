INSERT INTO pharmacyclaims_common_model
SELECT
    NULL,                                         -- record_id
    t.claim_id,                                   -- claim_id
    mp.hvid,                                      -- hvid
    NULL,                                         -- created
    '3',                                          -- model_version
    NULL,                                         -- data_set
    NULL,                                         -- data_feed
    NULL,                                         -- data_vendor
    NULL,                                         -- source_version
    mp.gender,                                    -- patient_gender
    mp.age,                                       -- patient_age
    mp.yearOfBirth,                               -- patient_year_of_birth
    mp.threeDigitZip,                             -- patient_zip3
    UPPER(mp.state),                              -- patient_state
    extract_date(
        t.filldate, '%Y-%m-%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                        -- date_service
    extract_date(
        t.writtendate, '%Y-%m-%d', CAST({min_date_written} AS DATE), CAST({max_date} AS DATE)
        ),                                        -- date_written
    t.date_injury,                                -- year_of_injury
    extract_date(
        t.date_authorized, '%Y-%m-%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                        -- date_authorized
    t.time_authorized,                            -- time_authorized
    t.transaction_code_std,                       -- transaction_code_std
    t.transaction_code_vendor,                    -- transaction_code_vendor
    t.response_code_std,                          -- response_code_std
    t.response_code_vendor,                       -- response_code_vendor
    t.reject_reason_code_1,                       -- reject_reason_code_1
    t.reject_reason_code_2,                       -- reject_reason_code_2
    t.reject_reason_code_3,                       -- reject_reason_code_3
    t.reject_reason_code_4,                       -- reject_reason_code_4
    t.reject_reason_code_5,                       -- reject_reason_code_5
    t.diagnosiscode,                              -- diagnosis_code
    CASE
    WHEN LOWER(t.qualifiertypetext) = 'icd-9'
    THEN '01'
    WHEN LOWER(t.qualifiertypetext) = 'icd-10'
    THEN '02'
    END,                                          -- diagnosis_code_qual
    t.procedure_code,                             -- procedure_code
    t.procedure_code_qual,                        -- procedure_code_qual
    t.ndccode,                                    -- ndc_code
    CASE
    WHEN t.product_service_id_qual <> '03'
    THEN t.product_service_id
    END,                                          -- product_service_id
    CASE
    WHEN t.product_service_id_qual <> '03'
    THEN t.product_service_id_qual
    END,                                          -- product_service_id_qual
    t.prescriptionid,                             -- rx_number
    t.rx_number_qual,                             -- rx_number_qual
    t.binnbr,                                     -- bin_number
    t.pcnnbr,                                     -- processor_control_number
    t.refillnbr,                                  -- fill_number
    t.writtenrefillcnt,                           -- refill_auth_amount
    t.fillquantity,                               -- dispensed_quantity
    t.unit_of_measure,                            -- unit_of_measure
    t.dayssupplydispensedcnt,                     -- days_supply
    t.npinbr,                                     -- pharmacy_npi
    t.prov_dispensing_npi,                        -- prov_dispensing_npi
    t.payer_id,                                   -- payer_id
    t.payer_id_qual,                              -- payer_id_qual
    t.billname,                                   -- payer_name
    t.payer_parent_name,                          -- payer_parent_name
    t.payer_org_name,                             -- payer_org_name
    t.payer_plan_id,                              -- payer_plan_id
    t.planname,                                   -- payer_plan_name
    t.payer_type,                                 -- payer_type
    t.compound_code,                              -- compound_code
    t.unit_dose_indicator,                        -- unit_dose_indicator
    t.dispenseaswrittenind,                       -- dispensed_as_written
    t.origintext,                                 -- prescription_origin
    t.submission_clarification,                   -- submission_clarification
    t.orig_prescribed_product_service_code,       -- orig_prescribed_product_service_code
    t.orig_prescribed_product_service_code_qual,  -- orig_prescribed_product_service_code_qual
    t.writtenqty,                                 -- orig_prescribed_quantity
    t.prior_auth_type_code,                       -- prior_auth_type_code
    t.level_of_service,                           -- level_of_service
    t.reason_for_service,                         -- reason_for_service
    t.professional_service_code,                  -- professional_service_code
    t.result_of_service_code,                     -- result_of_service_code
    t.prov_prescribing_npi,                       -- prov_prescribing_npi
    t.prov_primary_care_npi,                      -- prov_primary_care_npi
    t.cob_count,                                  -- cob_count
    t.usualandcustomary,                          -- usual_and_customary_charge
    t.product_selection_attributed,               -- product_selection_attributed
    t.other_payer_recognized,                     -- other_payer_recognized
    t.periodic_deductible_applied,                -- periodic_deductible_applied
    t.periodic_benefit_exceed,                    -- periodic_benefit_exceed
    t.accumulated_deductible,                     -- accumulated_deductible
    t.remaining_deductible,                       -- remaining_deductible
    t.remaining_benefit,                          -- remaining_benefit
    t.copay_coinsurance,                          -- copay_coinsurance
    t.basis_of_cost_determination,                -- basis_of_cost_determination
    t.costamt,                                    -- submitted_ingredient_cost
    t.dispensingfeeamt,                           -- submitted_dispensing_fee
    t.discountamt,                                -- submitted_incentive
    t.submitted_gross_due,                        -- submitted_gross_due
    t.submitted_professional_service_fee,         -- submitted_professional_service_fee
    t.copayamt,                                   -- submitted_patient_pay
    t.submitted_other_claimed_qual,               -- submitted_other_claimed_qual
    t.submitted_other_claimed,                    -- submitted_other_claimed
    t.basis_of_reimbursement_determination,       -- basis_of_reimbursement_determination
    t.payoramt,                                   -- paid_ingredient_cost
    t.paid_dispensing_fee,                        -- paid_dispensing_fee
    t.paid_incentive,                             -- paid_incentive
    t.paid_gross_due,                             -- paid_gross_due
    t.paid_professional_service_fee,              -- paid_professional_service_fee
    t.paid_patient_pay,                           -- paid_patient_pay
    t.paid_other_claimed_qual,                    -- paid_other_claimed_qual
    t.paid_other_claimed,                         -- paid_other_claimed
    t.tax_exempt_indicator,                       -- tax_exempt_indicator
    t.coupon_type,                                -- coupon_type
    t.coupon_number,                              -- coupon_number
    t.coupon_value,                               -- coupon_value
    t.ncpdpproviderid,                            -- pharmacy_other_id
    t.providertype,                               -- pharmacy_other_qu
    t.zipcode,                                    -- pharmacy_postal_code
    t.prov_dispensing_id,                         -- prov_dispensing id,
    t.prov_dispensing_qual,                       -- prov_dispensing_qual
    NULL,                                         -- prov_prescribing_id
    NULL,                                         -- prov_prescribing_qual
    NULL,                                         -- prov_primary_care_id
    NULL,                                         -- prov_primary_care_qual
    t.payorprioritynbr,                           -- other_payer_coverage_type
    t.other_payer_coverage_id,                    -- other_payer_coverage_id
    t.other_payer_coverage_qual,                  -- other_payer_coverage_qual
    t.other_payer_date,                           -- other_payer_date
    t.other_payer_coverage_code,                  -- other_payer_coverage_code
    t.logical_delete_reason                       -- logical_delete_reason
FROM transactions t

-- inner join will filter out rows with embedded commas - the
-- hvJoinKey will not exist in the matching_payload table
    INNER JOIN matching_payload mp ON t.hvJoinKey = mp.hvJoinKey
;

