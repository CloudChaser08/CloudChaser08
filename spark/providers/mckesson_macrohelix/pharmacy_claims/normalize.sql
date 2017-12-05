INSERT INTO pharmacyclaims_common_model
SELECT
        NULL,                                                                                   --record_id
        t.row_id,                                                                               --claim_id
        p.hvid,                                                                                 --hvid
        NULL,                                                                                   --created
        NULL,                                                                                   --model_version
        NULL,                                                                                   --data_set
        NULL,                                                                                   --data_feed
        NULL,                                                                                   --data_vendor
        NULL,                                                                                   --source_version
        CASE
            WHEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U'))) IN ('F', 'M', 'U')
                THEN UPPER(TRIM(COALESCE(p.gender, t.patient_gender, 'U')))
            ELSE 'U'
        END,                                                                                    --patient_gender
        NULL,                                                                                   --patient_age
        CASE
            WHEN (YEAR(t.service_date) - COALESCE(p.yearOfBirth, t.birth_date)) > 84 THEN 1927
            ELSE COALESCE(p.yearOfBirth, t.birthdate) 
        END,                                                                                    --patient_year_of_birth
        SUBSTR(TRIM(COALESCE(p.threeDigitZip, t.patient_zip)), 1, 3),                           --patient_zip3
        TRIM(UPPER(COALESCE(p.state, ''))),                                                     --patient_state
        t.service_date,                                                                         --date_service
        NULL,                                                                                   --date_written
        NULL,                                                                                   --year_of_injury
        NULL,                                                                                   --date_authorized
        NULL,                                                                                   --time_authorized
        NULL,                                                                                   --discharge_date
        NULL,                                                                                   --transaction_code_std
        NULL,                                                                                   --transaction_code_vendor
        NULL,                                                                                   --response_code_std
        NULL,                                                                                   --response_code_vendor
        NULL,                                                                                   --reject_reason_code_1
        NULL,                                                                                   --reject_reason_code_2
        NULL,                                                                                   --reject_reason_code_3
        NULL,                                                                                   --reject_reason_code_4
        NULL,                                                                                   --reject_reason_code_5
        CASE
            WHEN ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                       t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                       t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                       t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                       t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n]
                 IS NULL THEN NULL
            WHEN t.service_date < '2015-10-01'
                AND SUBSTR(TRIM(UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                       t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                       t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                       t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                       t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n])), 1, 1) = 'A'
                THEN SUBSTR(TRIM(UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                       t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                       t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                       t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                       t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n])), 2, 10)
            ELSE TRIM(UPPER(ARRAY(t.dx_01, t.dx_02, t.dx_03, t.dx_04, t.dx_05,
                       t.dx_06, t.dx_07, t.dx_08, t.dx_09, t.dx_10,
                       t.dx_11, t.dx_12, t.dx_13, t.dx_14, t.dx_15,
                       t.dx_16, t.dx_17, t.dx_18, t.dx_19, t.dx_20,
                       t.dx_21, t.dx_22, t.dx_23, t.dx_24)[e.n]))    
        END,                                                                                    --diagnosis_code
        NULL                           --diagnosis_code_qual
        NULL                           --procedure_code
        NULL                           --procedure_code_qual
        NULL                           --ndc_code
        NULL                           --product_service_id
        NULL                           --product_service_id_qual
        NULL                           --rx_number
        NULL                           --rx_number_qual
        NULL                           --bin_number
        NULL                           --processor_control_number
        NULL                           --fill_number
        NULL                           --refill_auth_amount
        NULL                           --dispensed_quantity
        NULL                           --unit_of_measure
        NULL                           --days_supply
        NULL                           --pharmacy_npi
        NULL                           --prov_dispensing_npi
        NULL                           --payer_id
        NULL                           --payer_id_qual
        NULL                           --payer_name
        NULL                           --payer_parent_name
        NULL                           --payer_org_name
        NULL                           --payer_plan_id
        NULL                           --payer_plan_name
        NULL                           --payer_type
        NULL                           --compound_code
        NULL                           --unit_dose_indicator
        NULL                           --dispensed_as_written
        NULL                           --prescription_origin
        NULL                           --submission_clarification
        NULL                           --orig_prescribed_product_service_code
        NULL                           --orig_prescribed_product_service_code_qual
        NULL                           --orig_prescribed_quantity
        NULL                           --prior_auth_type_code
        NULL                           --level_of_service
        NULL                           --reason_for_service
        NULL                           --professional_service_code
        NULL                           --result_of_service_code
        NULL                           --prov_prescribing_npi
        NULL                           --prov_primary_care_npi
        NULL                           --cob_count
        NULL                           --usual_and_customary_charge
        NULL                           --product_selection_attributed
        NULL                           --other_payer_recognized
        NULL                           --periodic_deductible_applied
        NULL                           --periodic_benefit_exceed
        NULL                           --accumulated_deductible
        NULL                           --remaining_deductible
        NULL                           --remaining_benefit
        NULL                           --copay_coinsurance
        NULL                           --basis_of_cost_determination
        NULL                           --submitted_ingredient_cost
        NULL                           --submitted_dispensing_fee
        NULL                           --submitted_incentive
        NULL                           --submitted_gross_due
        NULL                           --submitted_professional_service_fee
        NULL                           --submitted_patient_pay
        NULL                           --submitted_other_claimed_qual
        NULL                           --submitted_other_claimed
        NULL                           --basis_of_reimbursement_determination
        NULL                           --paid_ingredient_cost
        NULL                           --paid_dispensing_fee
        NULL                           --paid_incentive
        NULL                           --paid_gross_due
        NULL                           --paid_professional_service_fee
        NULL                           --paid_patient_pay
        NULL                           --paid_other_claimed_qual
        NULL                           --paid_other_claimed
        NULL                           --tax_exempt_indicator
        NULL                           --coupon_type
        NULL                           --coupon_number
        NULL                           --coupon_value
        NULL                           --pharmacy_other_id
        NULL                           --pharmacy_other_qual
        NULL                           --pharmacy_postal_code
        NULL                           --prov_dispensing_id
        NULL                           --prov_dispensing_qual
        NULL                           --prov_prescribing_id
        NULL                           --prov_prescribing_qual
        NULL                           --prov_primary_care_id
        NULL                           --prov_primary_care_qual
        NULL                           --other_payer_coverage_type
        NULL                           --other_payer_coverage_id 
        NULL                           --other_payer_coverage_qual
        NULL                           --other_payer_date
        NULL                           --other_payer_coverage_code
        NULL                           --logical_delete_reason
FROM mckesson_macrohelix_transactions t
LEFT OUTER JOIN payload p ON t.hvJoinKey = p.hvJoinKey
CROSS JOIN exploder e
;
