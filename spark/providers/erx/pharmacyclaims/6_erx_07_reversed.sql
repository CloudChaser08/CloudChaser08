SELECT
    txn.record_id,
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_year_of_birth,
    txn.patient_zip3,
    txn.patient_state,
    txn.date_service,
    txn.date_written,
    txn.year_of_injury,
    txn.date_authorized,
    txn.time_authorized,
    txn.transaction_code_std,
    txn.response_code_std,
    txn.reject_reason_code_1,
    txn.reject_reason_code_2,
    txn.reject_reason_code_3,
    txn.reject_reason_code_4,
    txn.reject_reason_code_5,
    txn.diagnosis_code,
    txn.diagnosis_code_qual,
    txn.procedure_code,
    txn.procedure_code_qual,
    txn.ndc_code,
    txn.rx_number,
    txn.rx_number_qual,
    txn.bin_number,
    txn.processor_control_number,
    txn.fill_number,
    txn.refill_auth_amount,
    txn.dispensed_quantity,
    txn.unit_of_measure,
    txn.days_supply,
    txn.pharmacy_npi,
    txn.prov_dispensing_npi,
    txn.payer_plan_id,
    txn.payer_plan_name,
    txn.compound_code,
    txn.unit_dose_indicator,
    txn.dispensed_as_written,
    txn.prescription_origin,
    txn.submission_clarification,
    txn.orig_prescribed_product_service_code,
    txn.orig_prescribed_product_service_code_qual,
    txn.orig_prescribed_quantity,
    txn.level_of_service,
    txn.reason_for_service,
    txn.professional_service_code,
    txn.result_of_service_code,
    txn.place_of_service_std_id,
    txn.prov_prescribing_npi,
    txn.prov_prescribing_name_1,
    txn.prov_prescribing_name_2,
    txn.prov_prescribing_address_1,
    txn.prov_prescribing_city,
    txn.prov_prescribing_state,
    txn.prov_prescribing_zip,
    txn.prov_primary_care_npi,
    txn.cob_count,
    txn.other_payer_recognized,
    txn.periodic_deductible_applied,
    txn.periodic_benefit_exceed,
    txn.accumulated_deductible,
    txn.remaining_deductible,
    txn.remaining_benefit,
    txn.copay_coinsurance,
    txn.basis_of_cost_determination,
    txn.submitted_ingredient_cost,
    txn.submitted_dispensing_fee,
    txn.submitted_incentive,
    txn.submitted_gross_due,
    txn.submitted_professional_service_fee,
    txn.submitted_patient_pay,
    txn.submitted_other_claimed_qual,
    txn.submitted_other_claimed,
    txn.basis_of_reimbursement_determination,
    txn.paid_ingredient_cost,
    txn.paid_dispensing_fee,
    txn.paid_incentive,
    txn.paid_gross_due,
    txn.paid_professional_service_fee,
    txn.paid_patient_pay,
    txn.paid_other_claimed_qual,
    txn.paid_other_claimed,
    txn.tax_exempt_indicator,
    txn.coupon_type,
    txn.coupon_number,
    txn.coupon_value,
    txn.other_payer_coverage_type,
    txn.other_payer_coverage_id,
    txn.other_payer_coverage_qual,
    txn.other_payer_date,
    txn.other_payer_coverage_code,
    'Reversed' AS logical_delete_reason,
    txn.part_provider,
    txn.part_best_date
FROM erx_06_comb_rownum txn
WHERE EXISTS
------------- Reversed Exist
(
    SELECT 1 FROM
      erx_05_reverse rev
      WHERE  
            COALESCE(txn.rx_number, 'NONE')            = COALESCE(rev.rx_number, '')
        AND COALESCE(txn.pharmacy_npi, rev.rx_number)  = COALESCE(rev.pharmacy_npi, txn.rx_number)
        AND COALESCE(txn.ndc_code, rev.rx_number)      = COALESCE(rev.ndc_code, txn.rx_number)
        AND COALESCE(txn.bin_number, rev.rx_number)    = COALESCE(rev.bin_number, txn.rx_number)
        AND COALESCE(CAST(txn.fill_number AS INT), rev.rx_number)  = COALESCE(CAST(rev.fill_number AS INT), txn.rx_number)
        AND UPPER(rev.logical_delete_reason)       = 'REVERSAL'
        AND txn.row_num = rev.row_num
        ------ Transaction table should be NULL for deleted reason
        AND txn.logical_delete_reason IS NULL


)
------------ Not Already Reversed 
AND NOT EXISTS
(
    SELECT 1 FROM
      erx_05_reverse rev
      WHERE
            COALESCE(txn.rx_number, 'NONE')            = COALESCE(rev.rx_number, '')
        AND COALESCE(txn.pharmacy_npi, rev.rx_number)  = COALESCE(rev.pharmacy_npi, txn.rx_number)
        AND COALESCE(txn.ndc_code, rev.rx_number)      = COALESCE(rev.ndc_code, txn.rx_number)
        AND COALESCE(txn.bin_number, rev.rx_number)    = COALESCE(rev.bin_number, txn.rx_number)
        AND COALESCE(CAST(txn.fill_number AS INT), rev.rx_number)  = COALESCE(CAST(rev.fill_number AS INT), txn.rx_number)
        AND txn.row_num = rev.row_num
        AND UPPER(txn.logical_delete_reason) = 'REVERSED'

)

--LIMIT 10
