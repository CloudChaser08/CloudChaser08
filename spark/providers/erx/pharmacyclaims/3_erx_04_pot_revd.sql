SELECT
     record_id,
     claim_id,
     hvid,
     created,
     model_version,
     data_set,
     data_feed,
     data_vendor,
     patient_gender,
     patient_year_of_birth,
     patient_zip3,
     patient_state,
     date_service,
     date_written,
     year_of_injury,
     date_authorized,
     time_authorized,
     transaction_code_std,
     response_code_std,
     reject_reason_code_1,
     reject_reason_code_2,
     reject_reason_code_3,
     reject_reason_code_4,
     reject_reason_code_5,
     diagnosis_code,
     diagnosis_code_qual,
     procedure_code,
     procedure_code_qual,
     ndc_code,
     rx_number,
     rx_number_qual,
     bin_number,
     processor_control_number,
     fill_number,
     refill_auth_amount,
     dispensed_quantity,
     unit_of_measure,
     days_supply,
     pharmacy_npi,
     prov_dispensing_npi,
     payer_plan_id,
     payer_plan_name,
     compound_code,
     unit_dose_indicator,
     dispensed_as_written,
     prescription_origin,
     submission_clarification,
     orig_prescribed_product_service_code,
     orig_prescribed_product_service_code_qual,
     orig_prescribed_quantity,
     level_of_service,
     reason_for_service,
     professional_service_code,
     result_of_service_code,
     place_of_service_std_id,
     prov_prescribing_npi,
     prov_prescribing_name_1,
     prov_prescribing_name_2,
     prov_prescribing_address_1,
     prov_prescribing_city,
     prov_prescribing_state,
     prov_prescribing_zip,
     prov_primary_care_npi,
     cob_count,
     other_payer_recognized,
     periodic_deductible_applied,
     periodic_benefit_exceed,
     accumulated_deductible,
     remaining_deductible,
     remaining_benefit,
     copay_coinsurance,
     basis_of_cost_determination,
     submitted_ingredient_cost,
     submitted_dispensing_fee,
     submitted_incentive,
     submitted_gross_due,
     submitted_professional_service_fee,
     submitted_patient_pay,
     submitted_other_claimed_qual,
     submitted_other_claimed,
     basis_of_reimbursement_determination,
     paid_ingredient_cost,
     paid_dispensing_fee,
     paid_incentive,
     paid_gross_due,
     paid_professional_service_fee,
     paid_patient_pay,
     paid_other_claimed_qual,
     paid_other_claimed,
     tax_exempt_indicator,
     coupon_type,
     coupon_number,
     coupon_value,
     other_payer_coverage_type,
     other_payer_coverage_id,
     other_payer_coverage_qual,
     other_payer_date,
     other_payer_coverage_code,
     logical_delete_reason,
     part_provider,
     part_best_date
 FROM erx_02_norm nrm
WHERE nrm.logical_delete_reason IS NULL

  /* Check to see if there's already a Reversed Claim that matches this row */
  /* (based on the reversal matching logic) in the newly normalized data. */
  /* With the current normalization logic, this should never happen, but */
  /* the logic is here, just in case the normalization logic changes. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM erx_02_norm rv1
        WHERE COALESCE(rv1.logical_delete_reason, '') = 'Reversed Claim'
          AND COALESCE(nrm.rx_number, '') = COALESCE(rv1.rx_number, '')
          AND COALESCE(nrm.pharmacy_npi, '') = COALESCE(rv1.pharmacy_npi, '')
          AND COALESCE(nrm.ndc_code, '') = COALESCE(rv1.ndc_code, '')
          AND COALESCE(nrm.bin_number, '') = COALESCE(rv1.bin_number, '')
          AND COALESCE(CAST(nrm.fill_number AS INT), '') = COALESCE(CAST(rv1.fill_number AS INT), '')
    )
  /* Check to see if there's already a Reversed Claim that matches this row */
  /* (based on the reversal matching logic) in the historical data. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM erx_03_hist rv2
        WHERE COALESCE(rv2.logical_delete_reason, '') = 'Reversed Claim'
          AND COALESCE(nrm.rx_number, '') = COALESCE(rv2.rx_number, '')
          AND COALESCE(nrm.pharmacy_npi, '') = COALESCE(rv2.pharmacy_npi, '')
          AND COALESCE(nrm.ndc_code, '') = COALESCE(rv2.ndc_code, '')
          AND COALESCE(nrm.bin_number, '') = COALESCE(rv2.bin_number, '')
          AND COALESCE(CAST(nrm.fill_number AS INT), '') = COALESCE(CAST(rv2.fill_number AS INT), '')
    ) 
