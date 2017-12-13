INSERT INTO pharmacyclaims_common_model
SELECT
    NULL,                            --record_id
    sk_utilization_key,              --claim_id
    NULL,                            --hvid
    NULL,                            --created
    '3',                             --model_version
    NULL,                            --data_set
    NULL,                            --data_feed
    NULL,                            --data_vendor
    NULL,                            --source_version
    NULL,                            --patient_gender
    NULL,                            --patient_age
    NULL,                            --patient_year_of_birth
    NULL,                            --patient_zip3
    NULL,                            --patient_state
    dispense_dttm,                   --date_service
    effective_start_date,            --date_written
    NULL,                            --year_of_injury
    NULL,                            --date_authorized
    NULL,                            --time_authorized
    discharge_dttm,                  --discharge_date
    NULL,                            --transaction_code_std
    NULL,                            --transaction_code_vendor
    NULL,                            --response_code_std
    NULL,                            --response_code_vendor
    NULL,                            --reject_reason_code_1
    NULL,                            --reject_reason_code_2
    NULL,                            --reject_reason_code_3
    NULL,                            --reject_reason_code_4
    NULL,                            --reject_reason_code_5
    drg_code,                        --diagnosis_code
    '99',                            --diagnosis_code_qual
    NULL,                            --procedure_code
    NULL,                            --procedure_code_qual
    ndc,                             --ndc_code
    hdc,                             --product_service_id
    '99',                            --product_service_id_qual
    NULL,                            --rx_number
    NULL,                            --rx_number_qual
    NULL,                            --bin_number
    NULL,                            --processor_control_number
    NULL,                            --fill_number
    NULL,                            --refill_auth_amount
    qty,                             --dispensed_quantity
    uom_qty,                         --unit_of_measure
    NULL,                            --days_supply
    NULL,                            --pharmacy_npi
    NULL,                            --prov_dispensing_npi
    NULL,                            --payer_id
    NULL,                            --payer_id_qual
    NULL,                            --payer_name
    NULL,                            --payer_parent_name
    NULL,                            --payer_org_name
    NULL,                            --payer_plan_id
    NULL,                            --payer_plan_name
    NULL,                            --payer_type
    NULL,                            --compound_code
    NULL,                            --unit_dose_indicator
    NULL,                            --dispensed_as_written
    NULL,                            --prescription_origin
    NULL,                            --submission_clarification
    NULL,                            --orig_prescribed_product_service_code
    NULL,                            --orig_prescribed_product_service_code_qual
    NULL,                            --orig_prescribed_quantity
    NULL,                            --prior_auth_type_code
    NULL,                            --level_of_service
    NULL,                            --reason_for_service
    NULL,                            --professional_service_code
    NULL,                            --result_of_service_code
    NULL,                            --prov_prescribing_npi
    NULL,                            --prov_primary_care_npi
    NULL,                            --cob_count
    acq_cost,                        --usual_and_customary_charge
    NULL,                            --product_selection_attributed
    NULL,                            --other_payer_recognized
    NULL,                            --periodic_deductible_applied
    NULL,                            --periodic_benefit_exceed
    NULL,                            --accumulated_deductible
    NULL,                            --remaining_deductible
    NULL,                            --remaining_benefit
    NULL,                            --copay_coinsurance
    NULL,                            --basis_of_cost_determination
    extended_cost,                   --submitted_ingredient_cost
    NULL,                            --submitted_dispensing_fee
    NULL,                            --submitted_incentive
    revenue,                         --submitted_gross_due
    NULL,                            --submitted_professional_service_fee
    NULL,                            --submitted_patient_pay
    NULL,                            --submitted_other_claimed_qual
    NULL,                            --submitted_other_claimed
    NULL,                            --basis_of_reimbursement_determination
    NULL,                            --paid_ingredient_cost
    NULL,                            --paid_dispensing_fee
    NULL,                            --paid_incentive
    NULL,                            --paid_gross_due
    NULL,                            --paid_professional_service_fee
    NULL,                            --paid_patient_pay
    NULL,                            --paid_other_claimed_qual
    NULL,                            --paid_other_claimed
    NULL,                            --tax_exempt_indicator
    NULL,                            --coupon_type
    NULL,                            --coupon_number
    NULL,                            --coupon_value
    sk_client_type_key,              --pharmacy_other_id
    '99',                            --pharmacy_other_qual
    NULL,                            --pharmacy_postal_code
    NULL,                            --prov_dispensing_id
    NULL,                            --prov_dispensing_qual
    prescribing_physician_code,      --prov_prescribing_id
    '99',                            --prov_prescribing_qual
    physician_code,                  --prov_primary_care_id
    '99',                            --prov_primary_care_qual
    NULL,                            --other_payer_coverage_type
    NULL,                            --other_payer_coverage_id
    NULL,                            --other_payer_coverage_qual
    NULL,                            --other_payer_date
    NULL,                            --other_payer_coverage_code
    NULL,                            --logical_delete_reason
    patient_type,                    --patient_type_vendor
    outlier,                         --outlier_vendor
    monthly_patient_days,            --monthly_patient_days_vendor
    extended_fee,                    --extended_fee_vendor
    discharge,                       --discharges_vendor
    discharge_patient_days,          --discharge_patient_days_vendor
    total_patient_days,              --total_patient_days_vendor
    client_name,                     --pharmacy_name
    address1,                        --pharmacy_address
    service_area_description,        --pharmacy_service_area_vendor
    master_service_area_description  --pharmacy_master_service_area_vendor
FROM cardinal_dcoa_transactions;
