SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'195'                                                                                   AS data_feed,
	'609'                                                                                   AS data_vendor,
	/* patient_year_of_birth (age, date_service, year of birth)*/
	CAP_YEAR_OF_BIRTH
        (
            NULL,
            CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10), '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,

    /* date_service */
    CASE 
        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10), '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10), '%Y-%m-%d') AS DATE) 
    END                                                                                     AS date_service,
    txn.test_name	                                                                        AS test_ordered_name,
    txn.result	                                                                            AS result,
    CASE
        WHEN txn.abnormal_flags = 'A' THEN 'Y'
        WHEN txn.abnormal_flags = 'N' THEN 'N'
    ELSE NULL
    END                                                                                     AS abnormal_flag,
	'luminate'                                                                              AS part_provider,
    /* part_best_date */
    CASE 
        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10),'%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(SUBSTR(txn.date_of_service, 1, 10),'%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.date_of_service,1, 4), '-',
                    SUBSTR(txn.date_of_service,6, 2), '-01'
                )
	END                                                                                 AS part_best_date
FROM txn
LEFT OUTER JOIN matching_payload pay ON txn.hvJoinKey = pay.hvJoinKey
WHERE COALESCE(UPPER(txn.test_name), 'X') <> 'TEST_NAME'


--LIMIT 100
