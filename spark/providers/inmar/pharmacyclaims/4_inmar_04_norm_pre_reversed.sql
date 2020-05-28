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
    txn.patient_age,
    txn.patient_year_of_birth,
    txn.patient_zip3,
    txn.patient_state,
    txn.date_service,
    txn.date_written,
    txn.year_of_injury,
    txn.date_authorized,
    txn.time_authorized,    
    ---------------------------------Logic for the authorized dates
    CASE 
        WHEN    --We have reversal and original date and time authorized
            (
                (COALESCE(rev.date_authorized,'') <> '' AND COALESCE(rev.time_authorized,'') <> '' AND COALESCE(txn.date_authorized,'') <> '' AND COALESCE(txn.time_authorized,'') <> '')
            AND
                rev.date_authorized >= txn.date_authorized
            AND
                rev.time_authorized >= txn.time_authorized
            ) THEN 1
        WHEN    --We are missing a time authorized, but have the dates authorized.  Fall back to date authorized
            (
                (COALESCE(rev.date_authorized,'') <> '' AND COALESCE(txn.date_authorized,'') <> '')
            AND
                (COALESCE(rev.time_authorized, '') = '' OR COALESCE(txn.time_authorized, '') = '')
            AND
                rev.date_authorized >= txn.date_authorized
            ) THEN 1
        WHEN    --We are missing a reversal or original date authorized.  Eliminate this creteria for matching
            (
                COALESCE(rev.date_authorized, '') = '' OR COALESCE(txn.date_authorized, '') = ''
            ) THEN 1
        ELSE 0
    END AS  authorized_date_logic,
    ---------------------------------
    
    txn.discharge_date,
    txn.transaction_code_std,
    txn.transaction_code_vendor,
    txn.response_code_std,
    txn.response_code_vendor,
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
    txn.product_service_id,
    txn.product_service_id_qual,
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
    txn.payer_id,
    txn.payer_id_qual,
    txn.payer_name,
    txn.payer_parent_name,
    txn.payer_org_name,
    txn.payer_plan_id,
    txn.payer_plan_name,
    txn.payer_type,
    txn.compound_code,
    txn.unit_dose_indicator,
    txn.dispensed_as_written,
    txn.prescription_origin,
    txn.submission_clarification,
    txn.orig_prescribed_product_service_code,
    txn.orig_prescribed_product_service_code_qual,
    txn.orig_prescribed_quantity,
    txn.prior_auth_type_code,
    txn.level_of_service,
    txn.reason_for_service,
    txn.professional_service_code,
    txn.result_of_service_code,
    txn.prov_prescribing_npi,
    txn.prov_prescribing_tax_id,
    txn.prov_prescribing_dea_id,    
    txn.prov_prescribing_ssn,
    txn.prov_prescribing_state_license,
    txn.prov_prescribing_upin,
    txn.prov_prescribing_commercial_id,
    txn.prov_prescribing_name_1,
    txn.prov_prescribing_name_2,
    txn.prov_prescribing_address_1,
    txn.prov_prescribing_address_2,
    txn.prov_prescribing_city,
    txn.prov_prescribing_state,
    txn.prov_prescribing_zip,
    txn.prov_prescribing_std_taxonomy,
    txn.prov_prescribing_vendor_specialty,
    txn.prov_primary_care_npi,
    txn.cob_count,
    txn.usual_and_customary_charge,
    txn.product_selection_attributed,
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
    txn.paid_dispensing_fee ,
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
    txn.pharmacy_other_id,
    txn.pharmacy_other_qual,
    txn.pharmacy_postal_code,
    txn.prov_dispensing_id,
    txn.prov_dispensing_qual,
    txn.prov_prescribing_id,
    txn.prov_prescribing_qual,
    txn.prov_primary_care_id,
    txn.prov_primary_care_qual,
    txn.other_payer_coverage_type,
    txn.other_payer_coverage_id,
    txn.other_payer_coverage_qual,
    txn.other_payer_date,
    txn.other_payer_coverage_code,
    txn.logical_delete_reason,
    txn.part_provider,
    txn.part_best_date,
      ROW_NUMBER() OVER (PARTITION BY 
                txn.rx_number,
                txn.date_service,
                txn.bin_number,
                txn.processor_control_number,
                txn.pharmacy_other_id,
                txn.fill_number,
                txn.dispensed_quantity,
                txn.days_supply,
                txn.paid_patient_pay,
                txn.transaction_code_std

        ORDER BY 
                txn.rx_number,
                txn.date_service,
                txn.bin_number,
                txn.processor_control_number,
                txn.pharmacy_other_id,
                txn.fill_number,
                txn.dispensed_quantity,
                txn.days_supply,
                txn.paid_patient_pay,
                txn.transaction_code_std

        ) AS row_num    
FROM inmar_03_comb_hist_cf txn
  INNER JOIN inmar_04_norm_reverse_status rev
        ON   COALESCE(rev.rx_number,'NONE')                                        = COALESCE(txn.rx_number,'')
        AND  COALESCE(rev.date_service, CONCAT('NONE', rev.rx_number))             = COALESCE(txn.date_service, CONCAT('NONE', txn.rx_number))
        AND  COALESCE(rev.bin_number, CONCAT('NONE', rev.rx_number))               = COALESCE(txn.bin_number,CONCAT('NONE', txn.rx_number))
        AND  COALESCE(rev.processor_control_number, CONCAT('NONE', rev.rx_number)) = COALESCE(txn.processor_control_number, CONCAT('NONE', txn.rx_number))
        AND  COALESCE(rev.pharmacy_other_id, CONCAT('NONE', rev.rx_number))        = COALESCE(txn.pharmacy_other_id,CONCAT('NONE', txn.rx_number))
        AND (COALESCE(rev.fill_number, CONCAT('NONE', rev.rx_number))              = COALESCE(txn.fill_number,CONCAT('NONE', txn.rx_number))          OR COALESCE(rev.fill_number,CONCAT('NONE', rev.rx_number)) = CONCAT('NONE', rev.rx_number) OR rev.fill_number = '0' )
        AND (COALESCE(rev.dispensed_quantity, CONCAT('NONE', rev.rx_number))       = COALESCE(txn.dispensed_quantity, CONCAT('NONE', txn.rx_number))  OR COALESCE(rev.dispensed_quantity,CONCAT('NONE', rev.rx_number)) = CONCAT('NONE', rev.rx_number) )
        AND (COALESCE(rev.days_supply, CONCAT('NONE', rev.rx_number))              = COALESCE(txn.days_supply,CONCAT('NONE', txn.rx_number))          OR rev.days_supply        = '0')
        --- ADD LOGIC FOR DATE AND TIME AUTHORIZED
        AND
              --We have reversal and original date and time authorized
        (
                (
                    (COALESCE(rev.date_authorized,'') <> '' AND COALESCE(rev.time_authorized,'') <> '' AND COALESCE(txn.date_authorized,'') <> '' AND COALESCE(txn.time_authorized,'') <> '')
                AND
                    rev.date_authorized >= txn.date_authorized
                AND
                    rev.time_authorized >= txn.time_authorized
                ) 
            OR    --We are missing a time authorized, but have the dates authorized.  Fall back to date authorized
                (
                    (COALESCE(rev.date_authorized,'') <> '' AND COALESCE(txn.date_authorized,'') <> '')
                AND
                    (COALESCE(rev.time_authorized, '') = '' OR COALESCE(txn.time_authorized, '') = '')
                AND
                    rev.date_authorized >= txn.date_authorized
                ) 
            OR    --We are missing a reversal or original date authorized.  Eliminate this creteria for matching
                (
                    COALESCE(rev.date_authorized, '') = '' OR COALESCE(txn.date_authorized, '') = ''
                )
        )
        
        --- ADD LOGIC FOR DATE AND TIME AUTHORIZED
        AND COALESCE(txn.transaction_code_std,'') = 'B1'
        AND COALESCE(txn.response_code_std,'')  NOT IN ( 'R','S') 

GROUP BY
1,	2,	
3,	4,	5,	6,	7,	8,	9,	10,	11,	12,	13,	14,	15,	16,	17,	18,	19,	20,	21,	22,	23,	24,	25,	26,	27,	28,	29,	30,										
31,	32,	33,	34,	35,	36,	37,	38,	39,	40,	41,	42,	43,	44,	45,	46,	47,	48,	49,	50,	51,	52,	53,	54,	55,	56,	57,	58,	59,	60,										
61,	62,	63,	64,	65,	66,	67,	68,	69,	70,	71,	72,	73,	74,	75,	76,	77,	78,	79,	80,	81,	82,	83,	84,	85,	86,	87,	88,	89,	90,										
91,	92,	93,	94,	95,	96,	97,	98,	99,	100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110,	
111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134
