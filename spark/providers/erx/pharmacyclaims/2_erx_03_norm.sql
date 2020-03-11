SELECT

    CONCAT('159', '_', txn.switch_transaction_number,'_', txn.product_service_id)           AS claim_id,    
    txn.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'159'                                                                                   AS data_feed,
	'515'                                                                                   AS data_vendor,
	/* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(COALESCE(txn.patient_gender_code, 'U')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(txn.patient_gender_code, 'U')), 1, 1)
        WHEN SUBSTR(UPPER(COALESCE(txn.gender             , 'U')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(txn.gender             , 'U')), 1, 1)
        ELSE 'U'
    END                                                                                     AS patient_gender,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            txn.age,
            COALESCE(CAST(EXTRACT_DATE(txn.date_of_fill,'%Y%m%d') AS DATE) , CAST('{VDR_FILE_DT}' AS DATE)),
            txn.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(COALESCE(txn.pt_zip_address, txn.threedigitzip), 1, 3))            AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.pt_state_address, txn.state, '')))               AS patient_state,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_of_fill,'%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_service,
    /* date_written */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_rx_written,'%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_written,
    YEAR(CAST(EXTRACT_DATE(txn.date_of_injury,'%Y%m%d') AS DATE))                          AS year_of_injury,
    /* date_authorized */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(SUBSTR(txn.date_switched,1,10),'%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_authorized,
    SUBSTR(txn.date_switched,12,8)                                                          AS time_authorized,
    txn.type_column			                                                                AS transaction_code_std,
    txn.transaction_response_status			                                                AS response_code_std,
    txn.reject_codes			                                                            AS reject_reason_code_1,
    txn.reject_codes_2			                                                            AS reject_reason_code_2,
    txn.reject_codes_3			                                                            AS reject_reason_code_3,
    txn.reject_codes_4			                                                            AS reject_reason_code_4,
    txn.reject_codes_5			                                                            AS reject_reason_code_5,
    CLEAN_UP_DIAGNOSIS_CODE
    (
        txn.diagnosis_code,
        txn.diagnosis_code_qualifier,
        CAST(EXTRACT_DATE(txn.date_of_fill,'%Y%m%d') AS DATE)
    )                                                                                       AS diagnosis_code,
    CASE
        WHEN txn.diagnosis_code IS NULL THEN NULL
    ELSE
       txn.diagnosis_code_qualifier
    END                                                                                     AS diagnosis_code_qual,
    -------------------- There are NDC with incorrect qualifier
    /* procedure_code */
    CASE
        WHEN txn.product_service_qualifier IN ('07', '08') AND LENGTH(txn.product_service_id) <> 11
        THEN txn.product_service_id
    ELSE
        NULL
    END                                                                                     AS procedure_code,
   /* procedure_code_qual */
    CASE
        WHEN txn.product_service_qualifier IN ('07', '08') AND LENGTH(txn.product_service_id) <> 11
        THEN txn.product_service_qualifier
    ELSE
        NULL
    END                                                                                     AS procedure_code_qual,
      -------------------- There are NDC with incorrect qualifier
    /* ndc_code */
    CLEAN_UP_NDC_CODE
    (
        CASE
            WHEN txn.product_service_qualifier IN ('00','03','07', '08') OR LENGTH(txn.product_service_id) = 11
            THEN txn.product_service_id
        ELSE
            NULL
        END
    )                                                                                       AS ndc_code,

    txn.rx_number                                                   AS rx_number,
    CASE
        WHEN txn.rx_number IS NOT NULL
        THEN 'RX_NUMBER'
    ELSE
        NULL
    END                                                                                     AS rx_number_qual,
    txn.bin			                                                                        AS bin_number,
    txn.pcn			                                                                        AS processor_control_number,
    txn.fill_number			                                                                AS fill_number,
    txn.number_of_refills_authorized			                                            AS refill_auth_amount,
    txn.quantity_dispensed			                                                        AS dispensed_quantity,
    txn.unit_of_measure			                                                            AS unit_of_measure,
    txn.days_supply			                                                                AS days_supply,
    ---- No cleanup needed as this is masked NPI
    pharmacy_npi                                                                            AS pharmacy_npi,
    CLEAN_UP_NPI_CODE(txn.service_number)	                                                AS prov_dispensing_npi,

    txn.plan			                                                                    AS payer_plan_id,
    txn.plan_description                                                                    AS payer_plan_name,
    txn.compound_code			                                                            AS compound_code,
    txn.unit_dose_indicator			                                                        AS unit_dose_indicator,
    txn.daw_code			                                                                AS dispensed_as_written,
    txn.rx_origin_code			                                                            AS prescription_origin,
    txn.submission_clarification_code			                                            AS submission_clarification,
    txn.orig_prescribed_prod_svc_code			                                            AS orig_prescribed_product_service_code,
   /* orig_prescribed_product_service_code_qual */
    CASE
        WHEN txn.orig_prescribed_prod_svc_code IS NOT NULL
        THEN txn.orig_prescribed_prod_svc_qualifier
    ELSE
        NULL
    END                                                                                     AS orig_prescribed_product_service_code_qual,

    txn.original_prescribed_quantity			                                            AS orig_prescribed_quantity,
    /* level_of_service LPAD(str,len,padstr) */
    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'
        THEN NULL
    ELSE
        txn.level_of_service
    END                                                                                     AS level_of_service,
    txn.reason_for_service_code			                                                    AS reason_for_service,
    txn.professional_service_code			                                                AS professional_service_code,
    txn.result_of_service_code			                                                    AS result_of_service_code,
    /* place_of_service_std_id */
    CASE
        WHEN txn.place_of_service_code IS NULL
            THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
            THEN '99'
        ELSE LPAD(txn.place_of_service_code, 2, 0)
    END	                                                                                    AS place_of_service_std_id,
    /* JKS 12/19/19 -  COALESCE(txn.prescriber_id, txn.prescriber_id_duplicate) was added per v03 mapping*/
    CLEAN_UP_NPI_CODE
    (
    CASE
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE COALESCE(txn.prescriber_id, txn.prescriber_id_duplicate)
    END
    ) 			                                                                            AS prov_prescribing_npi,
    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_last_name
    END                                                                                     AS prov_prescribing_name_1,
    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_first_name
    END                                                                                     AS prov_prescribing_name_2,

    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_street_address
    END                                                                                     AS prov_prescribing_address_1,

    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_city_address
    END                                                                                     AS prov_prescribing_city,

    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_state_address
    END                                                                                     AS prov_prescribing_state,

    CASE
        WHEN LPAD(txn.level_of_service, 2, 0) ='06'                                                          THEN NULL
        WHEN LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33') THEN NULL
        ELSE txn.prescriber_zip_address
    END                                                                                     AS prov_prescribing_zip,
    CLEAN_UP_NPI_CODE(txn.primary_care_provider_id)		                                    AS prov_primary_care_npi,
    txn.cob_other_payment_count			                                                    AS cob_count,
    txn.other_payer_amount_recognized_raw			                                        AS other_payer_recognized,
    txn.amount_applied_to_periodic_deductible			                                    AS periodic_deductible_applied,
    txn.amount_exceeding_periodic_benefit_maximum			                                AS periodic_benefit_exceed,
    txn.accumulated_deductible_paid			                                                AS accumulated_deductible,
    txn.remaining_deductible_amount			                                                AS remaining_deductible,
    txn.remaining_benefit_amount			                                                AS remaining_benefit,
    txn.amount_of_copay_coinsurance			                                                AS copay_coinsurance,
    CASE
        WHEN 0 <> LENGTH(COALESCE(txn.basis_cost_determination, ''))
        THEN LPAD(txn.basis_cost_determination, 2,'0')
    ELSE NULL
    END                                                                                     AS basis_of_cost_determination,
    txn.ingredient_cost			                                                            AS submitted_ingredient_cost,
    txn.dispensing_fee			                                                            AS submitted_dispensing_fee,
    txn.incentive_amount			                                                        AS submitted_incentive,
    txn.gross_amount_due			                                                        AS submitted_gross_due,
    txn.professional_service_fee			                                                AS submitted_professional_service_fee,
    txn.pt_paid_amount			                                                            AS submitted_patient_pay,
    CASE
        WHEN txn.other_amount_claimed_submitted IS NOT NULL
        THEN txn.other_amount_claimed_qualifier
    ELSE NULL
    END                                                                                     AS submitted_other_claimed_qual,
    txn.other_amount_claimed_submitted			                                            AS submitted_other_claimed,
    txn.basis_of_reimbursement_determination		        	                            AS basis_of_reimbursement_determination,
    txn.ingredient_cost_paid			                                                    AS paid_ingredient_cost,
    txn.dispensing_fee_paid			                                                        AS paid_dispensing_fee,
    txn.incentive_amount_paid			                                                    AS paid_incentive,
    txn.total_amount_paid			                                                        AS paid_gross_due,
    txn.professional_service_fee_paid_raw			                                        AS paid_professional_service_fee,
    txn.patient_pay_amount			                                                        AS paid_patient_pay,
    CASE
        WHEN txn.other_amount_paid IS NOT NULL
        THEN txn.other_amount_paid_qualifier
    ELSE NULL
    END                                                                                     AS paid_other_claimed_qual,
    txn.other_amount_paid			                                                        AS paid_other_claimed,
    txn.tax_exempt_indicator			                                                    AS tax_exempt_indicator,
    txn.coupon_type			                                                                As coupon_type,
    txn.coupon_number			                                                            AS coupon_number,
    txn.coupon_value_amount			                                                        AS coupon_value,
    txn.other_payer_coverage_type			                                                AS other_payer_coverage_type,
    txn.other_payer_id			                                                            AS other_payer_coverage_id,
    CASE
        WHEN txn.other_payer_id IS NOT NULL
        THEN txn.other_payer_id_qualifier
    ELSE NULL
    END                                                                                     AS other_payer_coverage_qual,
    /* date_authorized */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(SUBSTR(txn.other_payer_date,1,10),'%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS other_payer_date,
    txn.other_coverage_code			                                                        AS other_payer_coverage_code,
    --------------------
    CASE
        WHEN transaction_response_status in ('R','C', 'D', 'Q') THEN 'Rejected'
        WHEN type_column = 'B2' AND transaction_response_status = 'A' THEN 'Reversal'
    ELSE NULL END                                                                           AS logical_delete_reason,
	'erx'                                                                                   AS part_provider,

    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.date_of_fill,'%Y%m%d') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.date_of_fill, 1, 4), '-',
                    SUBSTR(txn.date_of_fill, 5, 2), '-01'
                )
	END                                                                                 AS part_best_date
 FROM erx_01_dedup txn
