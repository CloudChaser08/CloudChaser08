SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    CONCAT(txn.accessionid,'_',txn.orderid_patientid)                                       AS claim_id,
    COALESCE(pay.hvid, COALESCE(txn.orderid_patientid, 'UNAVAILABLE'))                      AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'153'                                                                                   AS data_feed,
	'500'                                                                                   AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.patientgender,'')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(txn.patientgender), 1, 1)
        	    WHEN SUBSTR(UPPER(COALESCE(pay.gender,''       )), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender       ), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            NULL,
            CAST(EXTRACT_DATE(SUBSTR(txn.userdatetime,1,8), '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patientaddresssubentity, pay.state, '')))        AS patient_state,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(SUBSTR(txn.userdatetime,1,8), '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
    /* date_specimen */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(SUBSTR(txn.drawdate_drawtime,1,8), '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_specimen,        
     /* date_report */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(SUBSTR(txn.reportfinal,1,8), '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_report,   
    SUBSTR(txn.reportfinal,9,5)                                                             AS time_report,   
    CLEAN_UP_LOINC_CODE(txn.loinccode)                                                      AS loinc_code,
    ---------- Not mapped yet---------
    txn.orderid_test                                                                        AS test_id,
    ---------- 
    txn.testid	                                                                            AS test_number,
    txn.ordertest_or_paneltest	                                                            AS test_ordered_name,
    txn.resulttestcode	                                                                    AS result_id,
    txn.resulttext	                                                                        AS result,
    txn.resulttestname	                                                                    AS result_name,
    txn.units_of_measure	                                                                AS result_unit_of_measure,
    txn.resultreferencerange	                                                            AS result_comments,
    txn.resultabnormalflag	                                                                AS ref_range_alpha,
    txn.fasting	                                                                            AS fasting_status,
    CLEAN_UP_DIAGNOSIS_CODE
    (
        txn.diagnosiscodes,
        NULL,
        CAST(EXTRACT_DATE(SUBSTR(txn.userdatetime,1,8), '%Y%m%d') AS DATE)
    )
                                                                                            AS diagnosis_code, 
    /* procedure_code */
    CLEAN_UP_PROCEDURE_CODE
    (    
    CASE
        WHEN txn.resultcpt IS NULL OR  txn.resultcpt = '99999'
            THEN NULL
    ELSE
        txn.resultcpt
    END 
    )                                                                                       AS procedure_code,
    /**procedure_code_qual */
    CASE
        WHEN txn.resultcpt IS NULL OR  txn.resultcpt = '99999'
            THEN NULL
    ELSE
        'CPT'
    END                                                                                     AS procedure_code_qual,   
    CLEAN_UP_NPI_CODE(txn.orderingprovidernpi)                                              AS ordering_npi,
    COALESCE(txn.performinglab_performed, txn.performinglab_analyzed)                       AS lab_other_id,

    CASE
        WHEN txn.performinglab_performed IS NOT NULL THEN "PERFORMINGLAB_PERFORMED"
        WHEN txn.performinglab_analyzed  IS NOT NULL THEN "PERFORMINGLAB_ANALYZED"
    ELSE NULL
    END                                                                                     AS lab_other_qual, 
    COALESCE(txn.labaddress_performed, txn.labaddress_analyzed)	                            AS lab_other_address_1,
    COALESCE(txn.labcity_performed, txn.labcity_analyzed)	                                AS lab_other_city,
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.labstate_performed, txn.labstate_analyzed )))    AS lab_other_state,
    COALESCE(txn.labpostalcode_performed, txn.labpostalcode_analyzed)	                    AS lab_other_zip,
    txn.clientname_ordering_practice                                                        AS ordering_other_id,
    CASE
        WHEN txn.clientname_ordering_practice IS NOT NULL THEN "CLIENTNAME_ORDERING_PRACTICE"
    ELSE NULL
    END                                                                                     AS ordering_other_qual, 
    /* ordering_name */
    TRIM
    (
        CASE
            WHEN LENGTH(COALESCE(txn.orderingprovidernamefirst,'')) > 0 
                AND LENGTH(COALESCE(txn.orderingprovidernamemiddle,'')) > 0 
                AND LENGTH(COALESCE(txn.orderingprovidernamelast,'')) > 0
                THEN CONCAT(txn.orderingprovidernamelast, ', ',txn.orderingprovidernamefirst,', ', txn.orderingprovidernamemiddle , CONCAT(' ',COALESCE(orderingprovidersuffix,'')))
            WHEN LENGTH(COALESCE(txn.orderingprovidernamefirst,'')) > 0 
                AND LENGTH(COALESCE(txn.orderingprovidernamelast,'')) > 0
                THEN CONCAT(txn.orderingprovidernamelast, ', ', txn.orderingprovidernamefirst, CONCAT(' ',COALESCE(orderingprovidersuffix,'')))   
            WHEN LENGTH(COALESCE(txn.orderingprovidernamelast,'')) > 0
                THEN txn.orderingprovidernamelast              
        ELSE NULL
        END
    )                                                                                       AS ordering_name,
    txn.orderingprovidercodetype	                                                        AS ordering_specialty,
    txn.clientid	                                                                        AS ordering_vendor_id,
    txn.clientaddressstreet	                                                                AS ordering_address_1,
    txn.clientaddresscity	                                                                AS ordering_city,
    txn.clientaddresssubentity	                                                            AS ordering_state,
    txn.clientaddresspostalcode	                                                            AS ordering_zip,
    /* logical_delete_reason */
    CASE 
        WHEN COALESCE(txn.resultstatus,'') = 'C' THEN 'Corrected'
        WHEN COALESCE(txn.resultstatus,'') = 'P' THEN 'Preliminary'
        WHEN COALESCE(txn.resultstatus,'') = 'U' THEN 'Unknown'
    ELSE NULL
    END	                                                                                    AS logical_delete_reason,
	'bioreference'                                                                          AS part_provider,
	
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(SUBSTR(txn.userdatetime,1,8), '%Y%m%d') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.userdatetime,1, 4), '-',
                    SUBSTR(txn.userdatetime,5, 2), '-01'
                )
	END                                                                                 AS part_best_date
FROM bioreference_pvt_diag_cd_cpt txn
LEFT OUTER JOIN matching_payload pay ON txn.hvJoinKey = pay.hvJoinKey
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 153
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 153
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1    

        

--LIMIT 100
