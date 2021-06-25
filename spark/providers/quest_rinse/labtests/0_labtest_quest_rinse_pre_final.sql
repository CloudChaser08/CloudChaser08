SELECT
    CONCAT(rslt.accn_id, '_',COALESCE(rslt.dos_id,''))                                       AS claim_id,
    pay.hvid                                                                                 AS hvid,
    CAST(CURRENT_DATE() AS STRING)                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(rslt.input_file_name, '/')[SIZE(SPLIT(rslt.input_file_name, '/')) - 1]            AS data_set,
	'187'                                                                                   AS data_feed,
	'7'                                                                                     AS data_vendor,
	/* patient_gender */
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(txn.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gender), 1, 1)
        	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        	    ELSE NULL
        	END
	    )                                                                                   AS patient_gender,
	/* patient_age (Notes for me - if the age is null this target field will be NULL)*/
	CAP_AGE -- Convert age values >= 85 to 90
	    (
	    VALIDATE_AGE  -- Ensure that the age is within 2 years of its derivation from date_service and year_of_birth.
            (
                pay.age,
                CAST(EXTRACT_DATE(rslt.date_of_service, '%Y-%m-%d') AS DATE),
                pay.yearofbirth
            )
        )                                                                                   AS patient_age,
	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            VALIDATE_AGE(pay.age,CAST(EXTRACT_DATE(rslt.date_of_service, '%Y-%m-%d') AS DATE), pay.yearofbirth),
            CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                    AS patient_year_of_birth ,
	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                           AS patient_zip3          ,
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.pat_state, pay.state)))                           AS patient_state         ,
    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE) < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE) > '{VDR_FILE_DT}' THEN NULL
        ELSE CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE)
    END                                                                                      AS date_service          ,
    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_collection, 1, 10), '%Y-%m-%d') AS DATE) < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_collection, 1, 10), '%Y-%m-%d') AS DATE) > '{VDR_FILE_DT}' THEN NULL
        ELSE CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_collection, 1, 10), '%Y-%m-%d') AS DATE)
    END                                                                                      AS date_specimen          ,

    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(COALESCE(rslt.date_final_report, rslt.date_reported), 1, 10), '%Y-%m-%d') AS DATE) < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(SUBSTR(COALESCE(rslt.date_final_report, rslt.date_reported), 1, 10), '%Y-%m-%d') AS DATE) > '{VDR_FILE_DT}' THEN NULL
        ELSE CAST(EXTRACT_DATE(SUBSTR(COALESCE(rslt.date_final_report, rslt.date_reported), 1, 10), '%Y-%m-%d') AS DATE)
    END                                                                                      AS date_report           ,
    CASE
        WHEN SUBSTR(COALESCE(rslt.date_final_report, rslt.date_reported), 12) = '00:00:00' THEN NULL
        ELSE SUBSTR(COALESCE(rslt.date_final_report, rslt.date_reported), 12)
    END                                                                                      AS time_report           ,
    rslt.loinc_code                                                                          AS loinc_code            ,
    rslt.lab_id                                                                              AS lab_id                ,
    rslt.obs_id                                                                              AS test_id               ,
    CASE
        WHEN UPPER(rslt.requisition_number) = 'NONE' THEN NULL
    ELSE rslt.requisition_number
    END                                                                                       AS test_number           ,
     rslt.local_profile_code                                                                  AS test_battery_local_id ,
     rslt.standard_profile_code                                                               AS test_battery_std_id   ,
     rslt.profile_name                                                                        AS test_battery_name     ,
     rslt.idw_local_order_code                                                                AS test_ordered_local_id ,
     rslt.standard_order_code                                                                 AS test_ordered_std_id   ,
     rslt.order_name                                                                          AS test_ordered_name     ,
     rslt.local_result_code                                                                   AS result_id             ,
     rslt.result_value                                                                        AS result                ,
     rslt.result_name                                                                         AS result_name           ,
     rslt.units                                                                               AS result_unit_of_measure,
     rslt.result_type                                                                         AS result_desc           ,
     COALESCE(comm.res_comm_text, histcomm.res_comm_text)		                              AS result_comments       ,
     CASE
        WHEN rslt.ref_range_alpha IS NOT NULL                                                                     THEN rslt.ref_range_alpha
        WHEN rslt.ref_range_alpha IS NULL AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS NOT NULL  THEN CONCAT(rslt.ref_range_low,' - ', rslt.ref_range_high)
        WHEN rslt.ref_range_alpha IS NULL AND rslt.ref_range_low IS     NULL AND rslt.ref_range_high IS     NULL  THEN NULL
        WHEN rslt.ref_range_alpha IS NULL AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS NULL      THEN rslt.ref_range_low
        WHEN rslt.ref_range_low   IS NULL AND rslt.ref_range_high IS NOT NULL                                     THEN rslt.ref_range_high
    ELSE NULL
    END                                                                                      AS ref_range_alpha,
    rslt.abnormal_ind                                                                        AS abnormal_flag,
    CASE
        WHEN COALESCE(rslt.fasting_ind,'') IN ('Y', 'N', 'U') THEN rslt.fasting_ind
        ELSE NULL
    END	                                                                                     AS fasting_status,
    CLEAN_UP_DIAGNOSIS_CODE
            (diag.s_diag_code,
            CASE
                WHEN diag.s_icd_codeset_ind = '9'  THEN '01'
                WHEN diag.s_icd_codeset_ind = '10' THEN '02'
            ELSE NULL
            END,
            CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE)
            )
                                                                                             AS diagnosis_code ,
    CASE
        WHEN CLEAN_UP_DIAGNOSIS_CODE
        (diag.s_diag_code,
        CASE
            WHEN diag.s_icd_codeset_ind = '9'  THEN '01'
            WHEN diag.s_icd_codeset_ind = '10' THEN '02'
        ELSE NULL
        END,
        CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE)
        ) IS NULL                         THEN NULL
        WHEN diag.s_icd_codeset_ind = '9' THEN '01'
        WHEN diag.s_icd_codeset_ind = '0' THEN '02'
    ELSE NULL
    END                                                                                         AS diagnosis_code_qual  ,
    CLEAN_UP_PROCEDURE_CODE(rslt.cpt_code)                                                      AS procedure_code       ,
    CASE
     WHEN CLEAN_UP_PROCEDURE_CODE(rslt.cpt_code)  IS NOT NULL THEN 'CPT'
    ELSE NULL
    END                                                                                         AS procedure_code_qual   ,
    CLEAN_UP_NPI_CODE(rslt.npi)                                                                 AS ordering_npi          ,
    rslt.ins_id                                                                                 AS payer_id              ,
    CASE
        WHEN rslt.ins_id IS NULL THEN NULL
    ELSE 'VENDOR'
    END                                                                                         AS payer_id_qual         ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------      Need HIPAA scrub  payer_name-------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN (UPPER(rslt.company) LIKE '%JAIL%' OR UPPER(rslt.company) LIKE '%PRISON%') THEN NULL
        ELSE rslt.company
    END                                                                                         AS payer_name            ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------      Need HIPAA scrub   ordering_name   ------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN rslt.acct_name IS NOT NULL AND rslt.phy_name IS NOT NULL AND  UPPER(rslt.acct_name) <> UPPER(rslt.phy_name)
            THEN
                CASE
                    WHEN (UPPER(rslt.acct_name) LIKE '%JAIL%' OR UPPER(rslt.acct_name) LIKE '%PRISON%') THEN rslt.phy_name
                    WHEN (UPPER(rslt.phy_name) LIKE '%JAIL%' OR UPPER(rslt.phy_name) LIKE '%PRISON%') THEN rslt.acct_name
                    ELSE CONCAT(rslt.acct_name ,' - ', rslt.phy_name)
                END
        WHEN rslt.acct_name IS NOT NULL AND rslt.phy_name IS NOT NULL AND  UPPER(rslt.acct_name) = UPPER(rslt.phy_name)
            THEN
                CASE
                    WHEN (UPPER(rslt.phy_name) LIKE '%JAIL%' OR UPPER(rslt.phy_name) LIKE '%PRISON%') THEN NULL
                    ELSE rslt.phy_name
                END
        WHEN rslt.acct_name IS NOT NULL AND rslt.phy_name IS NULL
                AND (UPPER(rslt.acct_name) NOT LIKE '%JAIL%' OR UPPER(rslt.acct_name) NOT LIKE '%PRISON%') THEN rslt.acct_name
        WHEN rslt.acct_name IS NULL AND rslt.phy_name IS NOT NULL
                AND (UPPER(rslt.phy_name) NOT LIKE '%JAIL%' OR UPPER(rslt.phy_name) NOT LIKE '%PRISON%') THEN rslt.phy_name
    ELSE NULL

    END                                                                                         AS ordering_name,

    rslt.market_type                                                                            AS ordering_market_type  ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------      Need HIPAA scrub   ordering_specialty  --------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN (UPPER(rslt.client_specialty) LIKE '%JAIL%' OR UPPER(rslt.client_specialty) LIKE '%PRISON%') THEN NULL
        ELSE rslt.client_specialty
    END                                                                                         AS ordering_specialty    ,
    rslt.phy_id                                                                                 AS ordering_vendor_id,
    rslt.cmdm_licnum                                                                            AS ordering_state_license,
    rslt.upin                                                                                   AS ordering_upin         ,
    rslt.acct_address_1                                                                         AS ordering_address_1    ,
    rslt.acct_address_2                                                                         AS ordering_address_2    ,
    rslt.acct_city                                                                              AS ordering_city         ,
    VALIDATE_STATE_CODE(UPPER(rslt.acct_state))                                                 AS ordering_state        ,
    rslt.acct_zip                                                                               AS ordering_zip          ,
    CASE
        WHEN COALESCE(rslt.canceled_accn_ind,'') = 'C' THEN 'CANCELED'
        WHEN COALESCE(rslt.amended_report_ind,'') = 'C' THEN 'AMENDED'
        WHEN COALESCE(rslt.idw_report_change_status,'') = 'Y' THEN 'YES'
        ELSE NULL
    END                                                                                         AS logical_delete_reason ,
   CONCAT
        (
        rslt.unique_accession_id,
        COALESCE(CONCAT('_',    rslt.accn_id),''),
        COALESCE(CONCAT('_',    rslt.ord_seq),''),
        COALESCE(CONCAT('_',    rslt.res_seq),''),
        COALESCE(CONCAT('_',    rslt.ins_seq),'')
        )                                                                                       AS vendor_record_id      ,
    'quest_rinse'                                                                               AS part_provider         ,
    /* part_best_date */
    CASE
        WHEN 0 = LENGTH(COALESCE
                                (
                                    CAP_DATE
                                           (
                                               CAST(EXTRACT_DATE(SUBSTR(rslt.date_of_service, 1, 10), '%Y-%m-%d') AS DATE),
                                               COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                               CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                           ),
                                       ''
                                   ))
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                       SUBSTR(rslt.date_of_service, 1, 4), '-',
                       SUBSTR(rslt.date_of_service, 6, 2), '-01'
                   )
    END                                                                                        AS part_best_date


FROM order_result rslt
LEFT OUTER JOIN diagnosis         diag ON rslt.unique_accession_id = diag.unique_accession_id
LEFT OUTER JOIN transactions       txn  ON rslt.unique_accession_id = txn.unique_accession_id
LEFT OUTER JOIN matching_payload  pay  ON txn.hvjoinkey            = pay.hvJoinKey
LEFT OUTER JOIN result_comments comm ON rslt.dos_id = comm.dos_id
                          AND rslt.dos_yyyymm          = comm.dos_yyyymm
                          AND rslt.lab_code            = comm.lab_code
                          AND rslt.accn_id             = comm.accn_id
                          AND rslt.unique_accession_id = comm.unique_accession_id
                          AND rslt.ord_seq             = comm.ord_seq
                          AND rslt.res_seq             = comm.res_seq
LEFT OUTER JOIN result_comments_hist histcomm ON rslt.dos_id = histcomm.dos_id
                          AND rslt.dos_yyyymm          = histcomm.dos_yyyymm
                          AND rslt.lab_code            = histcomm.lab_code
                          AND rslt.accn_id             = histcomm.accn_id
                          AND rslt.unique_accession_id = histcomm.unique_accession_id
                          AND rslt.ord_seq             = histcomm.ord_seq
                          AND rslt.res_seq             = histcomm.res_seq

WHERE
 LOWER(COALESCE(rslt.unique_accession_id, '')) <> 'unique_accession_id'
AND
  (
      ---------------- 1. The COUNTRY is Valid (from patient) --------------------------------------
        ( LENGTH(TRIM(COALESCE(txn.pat_country , ''))) <> 0  AND UPPER(COALESCE(SUBSTR(txn.pat_country,1,2),'US')) = 'US')
      ---------------- 2a. The state is NULL or Valid  (from patient) ------------------------------
      OR ( LENGTH(TRIM(COALESCE(txn.pat_state   , ''))) <> 0 AND EXISTS (SELECT 1 FROM ref_geo_state sts WHERE UPPER(COALESCE(txn.pat_state, 'PA')) = sts.geo_state_pstl_cd) )
      ---------------- 2b. The state is NULL or Valid  (from result) ------------------------------
      OR ( LENGTH(TRIM(COALESCE(rslt.acct_state  , ''))) <> 0 AND EXISTS(SELECT 1 FROM ref_geo_state sts WHERE UPPER(COALESCE(rslt.acct_state, 'PA')) = sts.geo_state_pstl_cd) )
      ---------------- 2c. The state is NULL or Valid  (from pay load) ------------------------------
      OR ( LENGTH(TRIM(COALESCE(pay.state        , ''))) <> 0 AND  EXISTS(SELECT 1 FROM ref_geo_state sts WHERE UPPER(COALESCE(pay.state, 'PA')) = sts.geo_state_pstl_cd)      )

  )
AND COALESCE(rslt.reportable_results_ind,'') <> 'N'
AND COALESCE(rslt.confidential_order_ind,'') <> 'Y'
--LIMIT 10
