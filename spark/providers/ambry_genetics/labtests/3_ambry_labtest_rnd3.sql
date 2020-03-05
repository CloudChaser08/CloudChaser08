SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.claim_id                                                                            AS claim_id,    
    COALESCE(pay.hvid,txn.deidentified_id)                                                  AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'08'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'43'                                                                                    AS data_feed,
	'194'                                                                                   AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.gender,'')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(txn.gender), 1, 1)
        	    WHEN SUBSTR(UPPER(COALESCE(pay.gender,'')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender,
	    
	    
	/* patient_age - (Notes for me - Per our transformation - if the pay.age is NULL the patient_age becomes NULL)  */
	CAP_AGE
	(
	VALIDATE_AGE
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.order_date, '%m/%d/%Y') AS DATE),
            COALESCE(txn.date_of_birth, pay.yearofbirth)
        )
    )                                                                                       AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.order_date, '%m/%d/%Y') AS DATE),
            COALESCE(txn.date_of_birth, pay.yearofbirth)
        )                                                                                   AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            SUBSTR(COALESCE(txn.patient_zip, pay.threedigitzip), 1, 3)
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(txn.patient_state, pay.state, ''))
        )                                                                                   AS patient_state,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.order_date, '%m/%d/%Y') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
        
    /* test_ordered_name */
    --------------    
    txn.genes_tested                                                                        AS test_ordered_name,
    CLEAN_UP_DIAGNOSIS_CODE
    (
        CASE
            WHEN txn.icd_10_codes =  'N/A'
                THEN NULL
            ELSE
                txn.icd_10_codes
        END,
        '02',
        CAST(EXTRACT_DATE(txn.order_date, '%m/%d/%Y') AS DATE)
    )
                                                                                            AS diagnosis_code, 
    CASE
        WHEN txn.icd_10_codes =  'N/A' OR txn.icd_10_codes IS NULL
            THEN NULL
    ELSE
        '02'
    END                                                                                     AS diagnosis_code_qual, 
    -------------
    CLEAN_UP_NPI_CODE(txn.npi)                                                              AS ordering_npi,
    txn.contact_name                                                                        AS ordering_name,
    txn.organization_address                                                                AS ordering_address_1,
    VALIDATE_STATE_CODE(txn.organization_state)                                             AS ordering_state,
    txn.organization_zip                                                                    AS ordering_zip,
	'ambry'                                                                                 AS part_provider,
	
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.order_date, '%m/%d/%Y') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(EXTRACT_DATE(txn.order_date, '%m/%d/%Y'), 1, 4), '-',
                    SUBSTR(EXTRACT_DATE(txn.order_date, '%m/%d/%Y'), 6, 2), '-01'
                )
	END                                                                                 AS part_best_date
FROM ambry_pvt_genes_icd10 txn
LEFT OUTER JOIN matching_payload pay ON txn.hvJoinKey = pay.hvJoinKey 
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 43
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 43
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1

-- LIMIT 100
