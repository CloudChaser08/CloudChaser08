SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.claim_id                                                                            AS claim_id,    
    payload.hvid                                                                            AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'147'                                                                                   AS data_feed,
	'486'                                                                                   AS data_vendor,

	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.patient_gender,'')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(txn.patient_gender, '')), 1, 1)
        	    WHEN SUBSTR(UPPER(COALESCE(payload.gender    ,'')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(payload.gender    , '')), 1, 1)
        	    ELSE 'U' 
        	END
	    )   AS patient_gender,
	    
	/* patient_age */
	VALIDATE_AGE
        (
            payload.age,
            CAST(EXTRACT_DATE(txn.date_service, '%Y-%m-%d') AS DATE),
            COALESCE(YEAR(CAST(EXTRACT_DATE(txn.patient_date_of_birth , '%Y%m%d') AS DATE)), payload.yearofbirth)
        )                                                                                   AS patient_age,

	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            payload.age,
            CAST(EXTRACT_DATE(txn.date_service, '%Y-%m-%d') AS DATE),
            COALESCE(YEAR(CAST(EXTRACT_DATE(txn.patient_date_of_birth , '%Y%m%d') AS DATE)), payload.yearofbirth, YEAR(CAST(EXTRACT_DATE('1900-01-01', '%Y-%m-%d') AS DATE)))
        )                                                                                   AS patient_year_of_birth,

    /* patient_zip3 */
    MASK_ZIP_CODE(SUBSTR(payload.threedigitzip, 1, 3))                                      AS patient_zip3,

    /* patient_state */
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_address_state_code, payload.state, ''))) AS patient_state,

    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_service, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,

    /* date_written */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_written, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_written,

    /* year_of_injury */
    txn.year_of_injury                                                                      AS year_of_injury,

    /* date_authorized */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_authorized, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_authorized,
 
     /* time_authorized */   
    txn.time_authorized                                                                     AS time_authorized,

    /* discharge_date */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.discharge_date, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS discharge_date,
 
    txn.transaction_code_std                                                                AS transaction_code_std,
    txn.transaction_code_vendor                                                             AS transaction_code_vendor,
    txn.response_code_std                                                                   AS response_code_std,
    txn.response_code_vendor                                                                AS response_code_vendor,
    txn.reject_reason_code_1                                                                AS reject_reason_code_1,
    txn.reject_reason_code_2                                                                AS reject_reason_code_2,
    txn.reject_reason_code_3                                                                AS reject_reason_code_3,
    txn.reject_reason_code_4                                                                AS reject_reason_code_4,
    txn.reject_reason_code_5                                                                AS reject_reason_code_5,
    
    /* diagnosis_code */   
    CLEAN_UP_DIAGNOSIS_CODE
       (
          txn.diagnosis_code,
    	  txn.diagnosis_code_qual,
    	  CAST(EXTRACT_DATE(txn.date_service, '%Y-%m-%d') AS DATE)
    	)                                                                                   AS diagnosis_code,

    /* diagnosis_code_qual */   	
	CASE WHEN LENGTH(COALESCE(txn.diagnosis_code,'')) > 0 AND COALESCE(txn.diagnosis_code_qual ,'XX') IN ('01', '02') 
	       THEN txn.diagnosis_code_qual 
	ELSE NULL END                                                                            AS diagnosis_code_qual,

    CLEAN_UP_PROCEDURE_CODE(procedure_code)                                                  AS procedure_code,

    /* procedure_code_qual */   	
	CASE WHEN LENGTH(COALESCE(txn.procedure_code,'')) > 0  
	            THEN txn.procedure_code_qual 
	ELSE NULL END                                                                            AS procedure_code_qual,	
	
    CLEAN_UP_NDC_CODE(ndc_code)                                                              AS ndc_code,
    txn.product_service_id                                                                   AS product_service_id,

   /* product_service_id_qual */ 	
	CASE WHEN LENGTH(COALESCE(txn.product_service_id,'')) > 0 
	    THEN txn.product_service_id_qual 
	ELSE NULL END                                                                           AS product_service_id_qual,

    MD5(txn.rx_number)                                                                      AS rx_number,
   /* rx_number_qual */
    CASE WHEN LENGTH(COALESCE(txn.rx_number,'')) > 0  
	            THEN '1' 
	ELSE NULL END                                                                           AS rx_number_qual,
    txn.bin_number                                                                          AS bin_number,
    txn.processor_control_number                                                            AS processor_control_number,
    txn.fill_number                                                                         AS fill_number,
    txn.refill_auth_amount                                                                  AS refill_auth_amount,
    txn.dispensed_quantity                                                                  AS dispensed_quantity,
    txn.unit_of_measure                                                                     AS unit_of_measure,
    txn.days_supply	                                                                        AS days_supply,
    CLEAN_UP_NPI_CODE(txn.pharmacy_npi)                                                     AS pharmacy_npi,
    CLEAN_UP_NPI_CODE(txn.prov_dispensing_npi)                                              AS prov_dispensing_npi,
    txn.payer_id                                                                            AS payer_id,
    CASE WHEN LENGTH(COALESCE(txn.payer_id,'')) > 0  
	            THEN txn.payer_id_qual 
	ELSE NULL END                                                                           AS payer_id_qual,

    txn.payer_name                                                                          AS payer_name,
    txn.payer_parent_name                                                                   AS payer_parent_name,
    txn.payer_org_name                                                                      AS payer_org_name,
    txn.payer_plan_id                                                                       AS payer_plan_id,
    txn.payer_plan_name                                                                     AS payer_plan_name,
    txn.payer_type                                                                          AS payer_type,

    /* compound_code */
    CASE 
        WHEN txn.compound_code IN ('0','1', '2') 
            THEN txn.compound_code
    ELSE NULL END                                                                           AS compound_code,

    txn.unit_dose_indicator                                                                 AS unit_dose_indicator,
    CLEAN_UP_NUMERIC_CODE(txn.dispensed_as_written)                                         AS dispensed_as_written,
    txn.prescription_origin                                                                 AS prescription_origin,

    /* submission_clarification */
    CASE 
    WHEN CLEAN_UP_NUMERIC_CODE(txn.submission_clarification) BETWEEN 1 AND 99 
        THEN CLEAN_UP_NUMERIC_CODE(txn.submission_clarification)
    ELSE NULL END                                                                            AS submission_clarification,

    /* orig_prescribed_product_service_code JKS - Additional transformation addded 9/5/2019 */ 
    CASE
        WHEN txn.orig_prescribed_product_service_code ='00000000000' THEN NULL
        ELSE txn.orig_prescribed_product_service_code END                                   AS orig_prescribed_product_service_code,

    /* orig_prescribed_product_service_code_qual JKS - Additional transformation addded 9/5/2019 */ 
    CASE WHEN LENGTH(COALESCE(txn.orig_prescribed_product_service_code,'')) > 0 
            AND txn.orig_prescribed_product_service_code <> '00000000000'
	            THEN txn.orig_prescribed_product_service_code_qual 
	ELSE NULL END                                                                           AS orig_prescribed_product_service_code_qual,

    txn.orig_prescribed_quantity                                                            AS orig_prescribed_quantity,
    txn.prior_auth_type_code                                                                AS prior_auth_type_code,
    txn.level_of_service                                                                    AS level_of_service,
    txn.reason_for_service                                                                  AS reason_for_service,
    txn.professional_service_code                                                           AS professional_service_code,
    txn.result_of_service_code                                                              AS result_of_service_code,
    CLEAN_UP_NDC_CODE(txn.prov_prescribing_npi)                                             AS prov_prescribing_npi,

    /* prov_prescribing_tax_id */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_tax_id
    END                                                                                     AS prov_prescribing_tax_id,
    /* prov_prescribing_dea_id */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_dea_id
    END                                                                                     AS prov_prescribing_dea_id,

    /* prov_prescribing_ssn */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_ssn   
    END                                                                                     AS prov_prescribing_ssn,

    /* prov_prescribing_state_license */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_state_license   
    END                                                                                     AS prov_prescribing_state_license,

    /* prov_prescribing_upin */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_upin   
    END                                                                                     AS prov_prescribing_upin,

    /* prov_prescribing_commercial_id */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_commercial_id   
    END                                                                                     AS prov_prescribing_commercial_id,

    /* prov_prescribing_name_1 */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_name_1   
    END                                                                                     AS prov_prescribing_name_1,

    /* prov_prescribing_name_2 */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_name_2   
    END                                                                                     AS prov_prescribing_name_2,

    /* prov_prescribing_address_1 */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_address_1   
    END                                                                                     AS prov_prescribing_address_1,

    /* prov_prescribing_address_2 */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_address_2   
    END                                                                                     AS prov_prescribing_address_2,

    /* prov_prescribing_city */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE txn.prov_prescribing_city   
    END                                                                                     AS prov_prescribing_city,

    /* prov_prescribing_state */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' THEN NULL
        ELSE  VALIDATE_STATE_CODE(UPPER(COALESCE(txn.prov_prescribing_state,'')))   
    END                                                                                     AS prov_prescribing_state,

    /* prov_prescribing_zip JKS - Additional transformation addded 9/5/2019 */ 
    CASE 
        WHEN SUBSTR(COALESCE(txn.level_of_service,'XXX'),1,1) ='6' 
        AND prov_prescribing_zip ='00000' THEN NULL
        ELSE  txn.prov_prescribing_zip   
    END                                                                                     AS prov_prescribing_zip,

    txn.prov_prescribing_std_taxonomy                                                       AS prov_prescribing_std_taxonomy,
    txn.prov_prescribing_vendor_specialty                                                   AS prov_prescribing_vendor_specialty,
    CLEAN_UP_NPI_CODE(txn.prov_primary_care_npi)                                            AS prov_primary_care_npi,
    txn.cob_count                                                                           AS cob_count,
    txn.usual_and_customary_charge                                                          AS usual_and_customary_charge,
    txn.product_selection_attributed                                                         AS product_selection_attributed,
    txn.other_payer_recognized                                                              AS other_payer_recognized,
    txn.periodic_deductible_applied                                                         AS periodic_deductible_applied,
    txn.periodic_benefit_exceed                                                             AS periodic_benefit_exceed,
    txn.accumulated_deductible                                                              AS accumulated_deductible,
    txn.remaining_deductible                                                                AS remaining_deductible,
    txn.remaining_benefit                                                                   AS remaining_benefit,
    txn.copay_coinsurance                                                                   AS copay_coinsurance,

    /* basis_of_cost_determination */ 
    CASE WHEN 0 <> LENGTH(COALESCE(txn.basis_of_cost_determination, '')) 
        THEN SUBSTR(CONCAT('0', txn.basis_of_cost_determination), -2) 
    ELSE NULL END                                                                           AS basis_of_cost_determination,

    txn.submitted_ingredient_cost                                                           AS submitted_ingredient_cost,
    txn.submitted_dispensing_fee                                                            AS submitted_dispensing_fee,
    txn.submitted_incentive                                                                 AS submitted_incentive,
    txn.submitted_gross_due                                                                 AS submitted_gross_due,
    txn.submitted_professional_service_fee                                                  AS submitted_professional_service_fee,
    txn.submitted_patient_pay                                                               AS submitted_patient_pay,
    
    /* submitted_other_claimed_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.submitted_other_claimed,'')) > 0  
	            THEN txn.submitted_other_claimed_qual 
	ELSE NULL END                                                                           AS submitted_other_claimed_qual,

    txn.submitted_other_claimed                                                             AS submitted_other_claimed,
    txn.basis_of_reimbursement_determination                                                AS basis_of_reimbursement_determination,
    txn.paid_ingredient_cost                                                                AS paid_ingredient_cost,
    txn.paid_dispensing_fee                                                                 AS paid_dispensing_fee,
    txn.paid_incentive                                                                      AS paid_incentive,
    txn.paid_gross_due                                                                      AS paid_gross_due,
    txn.paid_professional_service_fee                                                       AS paid_professional_service_fee,
    txn.paid_patient_pay                                                                    AS paid_patient_pay,
    
    /* paid_other_claimed_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.paid_other_claimed,'')) > 0  
	            THEN txn.paid_other_claimed_qual 
	ELSE NULL END                                                                           AS paid_other_claimed_qual,

    txn.paid_other_claimed                                                                  AS paid_other_claimed,
    txn.tax_exempt_indicator                                                                AS tax_exempt_indicator,
    txn.coupon_type                                                                         AS coupon_type,
    txn.coupon_number                                                                       AS coupon_number,
    txn.coupon_value                                                                        AS coupon_value,
    txn.pharmacy_other_id                                                                   AS pharmacy_other_id,
    
    /* pharmacy_other_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.pharmacy_other_id,'')) > 0  
	            THEN txn.pharmacy_other_qual 
	ELSE NULL END                                                                           AS pharmacy_other_qual,
    txn.pharmacy_postal_code                                                                AS pharmacy_postal_code,
    txn.prov_dispensing_id                                                                  AS prov_dispensing_id,

    /* prov_dispensing_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.prov_dispensing_id,'')) > 0  
	            THEN txn.prov_dispensing_qual 
	ELSE NULL END                                                                           AS prov_dispensing_qual,    

    txn.prov_prescribing_id                                                                 AS prov_prescribing_id,
    
    /* prov_prescribing_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.prov_prescribing_id,'')) > 0  
	            THEN txn.prov_prescribing_qual 
	ELSE NULL END                                                                           AS prov_prescribing_qual,

    txn.prov_primary_care_id                                                                AS prov_primary_care_id,

    /* prov_primary_care_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.prov_primary_care_id,'')) > 0  
	            THEN txn.prov_primary_care_qual 
	ELSE NULL END                                                                           AS prov_primary_care_qual,

    txn.other_payer_coverage_type                                                           AS other_payer_coverage_type,
    txn.other_payer_coverage_id                                                             AS other_payer_coverage_id,
    
   /* other_payer_coverage_qual */ 
    CASE WHEN LENGTH(COALESCE(txn.other_payer_coverage_id,'')) > 0  
	            THEN txn.other_payer_coverage_qual 
	ELSE NULL END                                                                           AS other_payer_coverage_qual,

    txn.other_payer_date                                                                    AS other_payer_date,
    txn.other_payer_coverage_code                                                           AS other_payer_coverage_code,
 
    /* logical_delete_reason */    
    CASE WHEN txn.transaction_code_std = 'B2' AND COALESCE(txn.response_code_std,'X') NOT IN ( 'R','S')
            THEN 'Reversal' ELSE NULL END                                                   AS  logical_delete_reason,
            
	'inmar'                                                                                 AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.date_service, '%Y-%m-%d') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.date_service, 1, 4), '-',
                    SUBSTR(txn.date_service, 6, 2), '-01'
                )
	END                                                                                 AS part_best_date 
	
FROM transaction txn
LEFT OUTER JOIN matching_payload payload ON txn.hvjoinkey = payload.hvjoinkey
LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 147
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 147
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1
WHERE UPPER(txn.claim_id)  <>  'CLAIM_ID'
--LIMIT 10
