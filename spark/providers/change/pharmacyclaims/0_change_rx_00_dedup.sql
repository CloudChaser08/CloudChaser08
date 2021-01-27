SELECT
    txn.accumulated_deductible,
    txn.basis_of_cost_determination,
    txn.basis_of_reimbursement_determination,
    txn.bin_number,
    txn.claim_id,
    txn.cob_count,
    txn.compound_code,
    txn.copay_coinsurance,
    txn.coupon_number,
    txn.coupon_type,
    txn.coupon_value,
    txn.date_authorized,
    txn.date_injury,
    txn.date_service,
    txn.date_written,
    txn.days_supply,
    txn.diagnosis_code,
    txn.diagnosis_code_qual,
    txn.dispensed_as_written,
    txn.dispensed_quantity,
    txn.fill_number,
    txn.gender_code,
    txn.incentive_paid,
    txn.level_of_service,
    txn.ncpdp_number,
    txn.orig_prescribed_product_service_code,
    txn.orig_prescribed_product_service_code_qual,
    txn.orig_prescribed_quantity,
    txn.other_payer_coverage_code,
    txn.other_payer_coverage_id,
    txn.other_payer_coverage_qual,
    txn.other_payer_coverage_type,
    txn.other_payer_date,
    txn.other_payer_recognized,
    txn.paid_dispensing_fee,
    txn.paid_gross_due,
    txn.paid_ingredient_cost,
    txn.paid_other_claimed,
    txn.paid_other_claimed_qual,
    txn.paid_patient_pay,
    txn.paid_professional_service_fee,
    txn.patient_state_province,
    txn.patient_zip3,
    txn.payer_id,
    txn.payer_id_qual,
    txn.payer_plan_id,
    txn.payer_plan_name,
    txn.payer_type,
    txn.periodic_benefit_exceed,
    txn.periodic_deductible_applied,
    txn.pharmacy_npi,
    txn.pharmacy_postal_code,
    txn.prescriber_id,
    txn.prescriber_last_name,
    txn.prescription_origin,
    txn.primary_care_provider_id,
    txn.prior_auth_type_code,
    txn.processor_control_number,
    txn.product_selection_attributed,
    txn.product_service_id,
    txn.product_service_id_qualifier,
    txn.professional_service_code,
    txn.prov_dispensing_qual,
    txn.prov_prescribing_qual,
    txn.prov_primary_care_qual,
    txn.provider_id,
    txn.reason_for_service,
    txn.refill_auth_amount,
    txn.reject_reason_code_1,
    txn.reject_reason_code_2,
    txn.reject_reason_code_3,
    txn.reject_reason_code_4,
    txn.reject_reason_code_5,
    txn.remaining_benefit,
    txn.remaining_deductible,
    txn.response_code_std,
    txn.result_of_service_code,
    txn.rx_number,
    txn.service_provider_id,
    txn.service_provider_id_qualifier,
    txn.submission_clarification,
    txn.submitted_dispensing_fee,
    txn.submitted_gross_due,
    txn.submitted_incentive,
    txn.submitted_ingredient_cost,
    txn.submitted_other_claimed,
    txn.submitted_other_claimed_qual,
    txn.submitted_patient_pay,
    txn.submitted_professional_service_fee,
    txn.tax_exempt_indicator,
    txn.time_authorized,
    txn.transaction_code_std,
    txn.unit_dose_indicator,
    txn.unit_of_measure,
    txn.usual_and_customary_charge,
    txn.year_of_birth,
    payload.age,
    payload.gender,
    payload.hvid,
    payload.state,
    payload.threedigitzip,
    payload.yearofbirth,
    MAX(txn.input_file_name)                                                    as input_file_name
FROM claim txn
    LEFT OUTER JOIN matching_payload payload
        ON txn.claim_id = payload.claimid AND payload.hvid IS NOT NULL
WHERE
    TRIM(UPPER(COALESCE(txn.claim_id, 'empty'))) <> 'CLAIM_ID'
GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
    61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,
    81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,
    101,102