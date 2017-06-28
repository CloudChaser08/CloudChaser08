INSERT INTO {restriction_level}_pharmacyclaims_common_model
SELECT
    NULL,                                     -- record_id
    t.prescriptionkey,                        -- claim_id
    mp.hvid,                                  -- hvid
    NULL,                                     -- created
    1,                                        -- model_version
    NULL,                                     -- data_set
    NULL,                                     -- data_feed
    NULL,                                     -- data_vendor
    NULL,                                     -- source_version
    mp.gender,                                -- patient_gender
    mp.age,                                   -- patient_age
    mp.yearOfBirth,                           -- patient_year_of_birth
    mp.threeDigitZip,                         -- patient_zip3
    UPPER(mp.state),                          -- patient_state
    extract_date(
        t.datefilled, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_service
    extract_date(
        t.datewritten, '%m/%d/%Y', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_written
    NULL,                                     -- year_of_injury
    extract_date(
        t.claimtransactiondate, '%Y/%m/%d', CAST('1999-01-01' AS DATE), CAST({max_date} AS DATE)
        ),                                    -- date_authorized
    t.fillertransactiontime,                  -- time_authorized
    NULL,                                     -- transaction_code_std
    CASE
    WHEN t.rebillindicator = 'O' OR t.rebillindicator = 'N'
    THEN 'Original'
    WHEN t.rebillindicator = 'R' OR t.rebillindicator = 'Y'
    THEN 'Rebilled'
    END,                                      -- transaction_code_vendor
    t.camstatuscode,                          -- response_code_std
    NULL,                                     -- response_code_vendor
    t.rejectcode1,                            -- reject_reason_code_1
    t.rejectcode2,                            -- reject_reason_code_2
    t.rejectcode3,                            -- reject_reason_code_3
    t.rejectcode4,                            -- reject_reason_code_4
    t.rejectcode5,                            -- reject_reason_code_5
    t.diagnosiscode,                          -- diagnosis_code
    NULL,                                     -- diagnosis_code_qual
    t.procedurecode,                          -- procedure_code
    t.procedurecodequalifier,                 -- procedure_code_qual
    t.dispensedndcnumber,                     -- ndc_code
    t.fillerproductserviceid,                 -- product_service_id
    t.productserviceidqualifier,              -- product_service_id_qual
    t.prescriptionnumber,                     -- rx_number
    t.prescriptionnumberqualifier,            -- rx_number_qual
    t.binnumber,                              -- bin_number
    t.processcontrolnumber,                   -- processor_control_number
    t.refillcode,                             -- fill_number
    t.numberofrefillsauthorized,              -- refill_auth_amount
    t.metricquantity,                         -- dispensed_quantity
    t.unitofmeasure,                          -- unit_of_measure
    t.dayssupply,                             -- days_supply
    t.customernpinumber,                      -- pharmacy_npi
    t.pharmacistnpi,                          -- prov_dispensing_npi
    t.payerid,                                -- payer_id
    t.payeridqualifier,                       -- payer_id_qual
    t.payername,                              -- payer_name
    NULL,                                     -- payer_parent_name
    t.payerorgname,                           -- payer_org_name
    t.payerplanid,                            -- payer_plan_id
    t.planname,                               -- payer_plan_name
    t.payertype,                              -- payer_type
    t.compoundindicator,                      -- compound_code
    t.unitdoseindicator,                      -- unit_dose_indicator
    t.dispensedaswritten,                     -- dispensed_as_written
    t.originofrx,                             -- prescription_origin
    t.submissionclarificationcode,            -- submission_clarification
    t.prescribedndcnumber,                    -- orig_prescribed_product_service_code
    t.prescribedproductservicecodequalifier,  -- orig_prescribed_product_service_code_qual
    t.prescribedquantity,                     -- orig_prescribed_quantity
    t.priorauthtypecode,                      -- prior_auth_type_code
    t.levelofservice,                         -- level_of_service
    t.reasonforservice,                       -- reason_for_service
    t.professionalservicecode,                -- professional_service_code
    t.resultofservicecode,                    -- result_of_service_code
    t.prescribernpi,                          -- prov_prescribing_npi
    t.primarycareproviderid,                  -- prov_primary_care_npi
    t.cobcount,                               -- cob_count
    t.usualandcustomary,                      -- usual_and_customary_charge
-- (would be amountattribtosalestax)
    NULL,                                     -- sales_tax
    t.productselectionattributed,             -- product_selection_attributed
    t.otherpayeramountpaid,                   -- other_payer_recognized
    t.periodicdeductibleapplied,              -- periodic_deductible_applied
    t.periodicbenefitexceed,                  -- periodic_benefit_exceed
    t.accumulateddeductible,                  -- accumulated_deductible
    t.remainingdeductible,                    -- remaining_deductible
    t.remainingbenefit,                       -- remaining_benefit
    t.copayamount,                            -- copay_coinsurance
    t.basisofingredientcostsubmitted,         -- basis_of_cost_determination
    t.ingredientcostsubmitted,                -- submitted_ingredient_cost
    t.dispensingfeesubmitted,                 -- submitted_dispensing_fee
    t.incentiveamountsubmitted,               -- submitted_incentive
    t.grossamountdue,                         -- submitted_gross_due
    t.professionalservicefeesubmitted,        -- submitted_professional_service_fee
-- (would be flatsalestaxamtsubmitted)
    NULL,                                     -- submitted_flat_sales_tax
-- (would be basisofpercentagesalestaxsubmitted)
    NULL,                                     -- submitted_percent_sales_tax_basis
-- (would be percentagesalestaxratesubmitted)
    NULL,                                     -- submitted_percent_sales_tax_rate
-- (would be percentagesalestaxamountsubmitted)
    NULL,                                     -- submitted_percent_sales_tax_amount
    t.patientpayamountsubmitted,              -- submitted_patient_pay
    t.otheramountsubmittedqualfier,           -- submitted_other_claimed_qual
    NULL,                                     -- submitted_other_claimed
    t.basisofingredientcostpaid,              -- basis_of_reimbursement_determination
    t.ingredientcostpaid,                     -- paid_ingredient_cost
    t.dispensingfeepaid,                      -- paid_dispensing_fee
    t.IncentiveAmountPaid,                    -- paid_incentive
    t.grossamountduepaid,                     -- paid_gross_due
    t.professionalservicefeepaid,             -- paid_professional_service_fee
-- (would be flatsalestaxamountpaid)
    NULL,                                     -- paid_flat_sales_tax
-- (would be basisofpercentagesalestaxpaid)
    NULL,                                     -- paid_percent_sales_tax_basis
-- (would be percentagesalestaxratepaid)
    NULL,                                     -- paid_percent_sales_tax_rate
-- (would be percentagesalestaxamountpaid)
    NULL,                                     -- paid_percent_sales_tax
    t.patientpayamountpaid,                   -- paid_patient_pay
    t.otheramountpaidqualifier,               -- paid_other_claimed_qual
    t.paidotherclaimed,                       -- paid_other_claimed
    t.taxexemptindicator,                     -- tax_exempt_indicator
    t.coupontype,                             -- coupon_type
    t.couponidnumber,                         -- coupon_number
    t.couponfacevalue,                        -- coupon_value
    t.pharmacyotherid,                        -- pharmacy_other_id
    t.pharmacyotheridqualifier,               -- pharmacy_other_qual
    t.customerzipcode,                        -- pharmacy_postal_code
    t.providerdispensingnpi,                  -- prov_dispensing_id
    t.provdispensingqual,                     -- prov_dispensing_qual
    NULL,                                     -- prov_prescribing_id
    NULL,                                     -- prov_prescribing_qual
    t.primarycareproviderid,                  -- prov_primary_care_id
    t.primarycareprovideridqualifier,         -- prov_primary_care_qual
    t.otherpayercoveragetype,                 -- other_payer_coverage_type
    t.otherpayercoverageid,                   -- other_payer_coverage_id
    t.otherpayercoverageidqualifier,          -- other_payer_coverage_qual
    t.otherpayerdate,                         -- other_payer_date
    t.otherpayercoveragecode,                 -- other_payer_coverage_code
    CASE
    WHEN COALESCE(t.rejectcode1, '') != ''
    OR COALESCE(t.rejectcode2, '') != ''
    OR COALESCE(t.rejectcode3, '') != ''
    OR COALESCE(t.rejectcode4, '') != ''
    OR COALESCE(t.rejectcode5, '') != ''
    OR COALESCE(t.camstatuscode, 'X') = 'R'
    THEN 'Claim Rejected'
    END                                       -- logical_delete_reason
FROM {restriction_level}_transactions t
    LEFT JOIN matching_payload mp ON t.hvjoinkey = mp.hvjoinkey
;
