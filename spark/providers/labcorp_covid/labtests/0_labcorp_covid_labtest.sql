SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    /* claim_id */
    CASE
        WHEN txn.accession_id IS NOT NULL THEN txn.accession_id
        ELSE NULL
    END                                                                                     AS claim_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'201'                                                                                   AS data_feed,
	'10'                                                                                    AS data_vendor,
	/* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(txn.patient_sex), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(txn.patient_sex), 1, 1)
        WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE 'U'
    END   AS patient_gender,
	CAP_YEAR_OF_BIRTH
	(
	    pay.age,
	    TO_DATE(txn.pat_dos, 'yyyyMMdd'),
	    pay.yearofbirth
	)                                                                                       AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,

    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_st, pay.state)))                         AS patient_state,
    CASE
        WHEN TO_DATE(txn.pat_dos, 'yyyyMMdd') < CAST(${EARLIEST_VALID_SERVICE_DATE} AS DATE)
          OR TO_DATE(txn.pat_dos, 'yyyyMMdd') > CAST(${VDR_FILE_DT}                 AS DATE) THEN NULL
        ELSE TO_DATE(txn.pat_dos, 'yyyyMMdd')
    END                                                                                      AS date_service,  
    CLEAN_UP_LOINC_CODE(txn.loinc_code)                                                      AS loinc_code,
    txn.test_number                                                                          AS test_number,
    txn.test_ordered_code                                                                    AS test_ordered_std_id,
    txn.test_ordered_name                                                                    AS test_ordered_name,
    txn.result_abbrev	                                                                     AS result,
    txn.test_name                                                                            AS result_name,
    	/* result_desc */
    CASE
        WHEN txn.result_abn_code = 'A'
        THEN 'Alert' 
        WHEN txn.result_abn_code = 'C'
        THEN 'Critical' 
        ELSE NULL
    END                                                                                      AS result_desc,
    CLEAN_UP_NPI_CODE(txn.npi)                                                               AS ordering_npi,    
    txn.perf_lab_code                                                                        AS lab_other_id,
        /* lab_other_qual */
    CASE
        WHEN txn.perf_lab_code IS NOT NULL
        THEN 'PERFORMING_LAB'
        ELSE NULL
    END                                                                                      AS lab_other_qual,
    spec.specialty_description                                                               AS ordering_specialty,
    txn.report_zip                                                                           AS ordering_zip,
    'labcorp_covid'                                                                          AS part_provider,
    /* part_best_date */
        CASE
            WHEN TO_DATE(txn.pat_dos, 'yyyyMMdd') < CAST(${HVM_AVAILABLE_HISTORY_START_DATE} AS DATE)
              OR TO_DATE(txn.pat_dos, 'yyyyMMdd') > CAST(${VDR_FILE_DT}                 AS DATE) THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
        (
            SUBSTR(txn.pat_dos, 1, 4), '-',
            SUBSTR(txn.pat_dos, 5, 2), '-01'
        )
        END                                                                                     AS part_best_date 

FROM txn
LEFT OUTER JOIN matching_payload pay  ON txn.hvjoinkey      = pay.hvjoinkey 
LEFT OUTER JOIN labcorp_spec spec ON txn.specialty_code = spec.spec_code
