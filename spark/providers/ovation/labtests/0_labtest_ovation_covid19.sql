SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.requisition_id                                                                      AS claim_id, 
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'188'                                                                                   AS data_feed,
	'548'                                                                                   AS data_vendor,
	/* --------------------------------  patient_gender  -----------------------------------------------*/
    CASE
        WHEN SUBSTR(UPPER(txn.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gender), 1, 1)
        WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M')      THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE NULL
    END                                                                                    AS patient_gender,
	/* --------------------------------  patient_age  -----------------------------------------------*/    
    /* -------------------- Adding a CASE STATEMENT just to save processing time --------------------*/
    CASE 
        WHEN CAST(pay.age AS INTEGER) IS NULL THEN NULL
        WHEN CAST(pay.age AS INTEGER) = 0 AND (txn.sample_collection_date IS NULL OR txn.year_of_birth IS NULL) THEN NULL
        ELSE 
            CASE
                WHEN
                    VALIDATE_AGE
                        (
                            pay.age,
                            CAST(txn.sample_collection_date AS DATE),
                            COALESCE(txn.year_of_birth, pay.yearofbirth)
                        ) > 84 THEN 90
                ELSE
                    VALIDATE_AGE
                        (
                            pay.age,
                            CAST(txn.sample_collection_date AS DATE),
                            COALESCE(txn.year_of_birth, pay.yearofbirth)
                        ) 
            END
                                                                                           
    END                                                                                    AS patient_age,
	/* --------------------------------  patient_year_of_birth  ----------------------------------------*/    
    CASE
        WHEN CAST(COALESCE(txn.year_of_birth, pay.yearofbirth) AS INTEGER) = 0     THEN NULL 
        WHEN CAST(COALESCE(txn.year_of_birth, pay.yearofbirth) AS INTEGER) IS NULL THEN NULL
    ELSE
    	CAP_YEAR_OF_BIRTH
    	(
    	    pay.age,
    	    CAST(txn.sample_collection_date AS DATE),
    	    COALESCE(txn.year_of_birth, pay.yearofbirth)
    	)                                                                                      
	END                                                                                   AS patient_year_of_birth,
	/* --------------------------------  patient_zip3 --------------------------------------------------*/    
	CASE 
	    WHEN txn.patient_zip3 IS NULL AND pay.threedigitzip IS NULL THEN NULL
	    WHEN LENGTH(txn.patient_zip3) >= 3 AND CAST(txn.patient_zip3 AS INTEGER) <> 0 THEN 	MASK_ZIP_CODE(SUBSTR(txn.patient_zip3,1,3))
	ELSE MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))
	END                                                                                     AS patient_zip3,
	
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_state, pay.state)))                      AS patient_state,
	/* --------------------------------  date_service --------------------------------------------------*/    
    CASE 
        WHEN CAST(txn.sample_collection_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.sample_collection_date AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(txn.sample_collection_date AS DATE)
    END                                                                                      AS date_service,  
	/* --------------------------------  date_specimen --------------------------------------------------*/    
    CASE 
        WHEN CAST(txn.sample_collection_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.sample_collection_date AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(txn.sample_collection_date AS DATE)
    END                                                                                      AS date_specimen, 
	/* --------------------------------  date_report ----------------------------------------------------*/    
    CASE 
        WHEN CAST(txn.report_creation_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.report_creation_date AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(txn.report_creation_date AS DATE)
    END                                                                                      AS date_report, 
    CLEAN_UP_NUMERIC_CODE(txn.loinc_code)                                                    AS loinc_code,
    txn.lab_id                                                                               AS lab_id,
    txn.result	                                                                             AS result,
    txn.target_name	                                                                         AS result_name,
	/* --------------------------------  result_desc ---------------------------------------------------*/    
    CASE
    WHEN txn.kit_name   IS NOT NULL AND txn.instrument IS NOT NULL THEN CONCAT(txn.kit_name, ' : ', txn.instrument)
    WHEN txn.kit_name   IS NOT NULL THEN txn.kit_name
    WHEN txn.instrument IS NOT NULL THEN txn.instrument
    ELSE NULL
    END                                                                                        AS result_desc,

	/* --------------------------------  ordering_npi ---------------------------------------------------*/    
    CASE 
        WHEN CAST(txn.physician_npi AS INT) = 0  THEN NULL
        ELSE CLEAN_UP_NPI_CODE(txn.physician_npi)
    END                                                                                      AS ordering_npi,
    txn.payor_name                                                                           AS payer_name,
    txn.care_site_zip                                                                        AS lab_zip,
	/* --------------------------------  ordering_name --------------------------------------------------*/    
    CASE 
        WHEN SUBSTR(UPPER(txn.physician_name),1, 5) = 'TEST ' THEN NULL
        ELSE txn.physician_name
    END                                                                                      AS ordering_name,
    'ovation'                                                                                AS part_provider,
	/* --------------------------------  part_mth -------------------------------------------------------*/    
	CASE
	    WHEN 0 = LENGTH
	        (COALESCE
	            (
	           CAP_DATE
                (
                  CAST(txn.sample_collection_date                                                AS DATE), 
                  CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                  CAST('{VDR_FILE_DT}'                                                            AS DATE)
                ), 
                ''
                )
            )
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.sample_collection_date, 1, 4), '-',
                    SUBSTR(txn.sample_collection_date, 6, 2), '-01'
                )
	END                                                                                     AS part_best_date       


FROM txn
LEFT OUTER JOIN matching_payload pay ON txn.hvjoinkey  = pay.hvJoinKey
