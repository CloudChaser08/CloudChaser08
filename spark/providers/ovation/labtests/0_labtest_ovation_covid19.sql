SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.requisition_id                                                                      AS claim_id, 
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'188'                                                                                   AS data_feed,
	'548'                                                                                   AS data_vendor,
	/* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(txn.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(txn.gender), 1, 1)
        WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE 'U' 
    END                                                                                     AS patient_gender,
	CAP_YEAR_OF_BIRTH
	(
	    pay.age,
	    CAST(txn.sample_collection_date AS DATE),
	    COALESCE(txn.year_of_birth,pay.yearofbirth)
	)                                                                                       AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(COALESCE(txn.patient_zip3,pay.threedigitzip), 1, 3))               AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_state, pay.state)))                      AS patient_state,
    CASE 
        WHEN CAST(txn.sample_collection_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.sample_collection_date AS DATE) > CAST('{VDR_FILE_DT}'           AS DATE) THEN NULL
        ELSE CAST(txn.sample_collection_date AS DATE)
    END                                                                                     AS date_service,
    CASE 
        WHEN CAST(txn.sample_collection_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.sample_collection_date AS DATE) > CAST('{VDR_FILE_DT}'           AS DATE) THEN NULL
        ELSE CAST(txn.sample_collection_date AS DATE)
    END                                                                                     AS date_specimen,
    CASE 
        WHEN CAST(txn.report_creation_date AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(txn.report_creation_date AS DATE) > CAST('{VDR_FILE_DT}'           AS DATE) THEN NULL
        ELSE CAST(txn.report_creation_date AS DATE)
    END                                                                                      AS date_report,
    txn.result	                                                                             AS result,
    txn.target_name	                                                                         AS result_name,
    txn.payor_name                                                                           AS payer_name,
        ---------- care site zip added on 4/13/2020
    txn.care_site_zip                                                                        AS lab_zip,
    txn.care_site_name                                                                       AS ordering_other_id,
    'CLIENTNAME_ORDERING_PRACTICE'                                                           AS ordering_other_qual,
    ---------- CASE added to exclude NPI Number with 000000000 2020-04-08 Jugal S
    CASE
        WHEN CAST(txn.physician_npi AS INT) = 0  THEN NULL
        ELSE CLEAN_UP_NPI_CODE(txn.physician_npi)
    END                                                                                      AS ordering_npi,
    ---------- CASE added to exclude Physician Name with "Test" 2020-04-08 Jugal S
    CASE
        WHEN SUBSTR(UPPER(txn.physician_name),1, 5) = 'TEST ' THEN NULL
        ELSE txn.physician_name
    END                                                                                      AS ordering_name,
    'ovation'                                                                                AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH
	        (COALESCE
	            (
	           CAP_DATE
                (
                  CAST(txn.sample_collection_date                                    AS DATE),
                  CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE), 
                  CAST('{VDR_FILE_DT}'                                               AS DATE)
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

