SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CASE
        WHEN clm.claim_id IS NULL
            THEN NULL
        ELSE CONCAT
                (
                    '150_',
                    COALESCE(enc.hvid, 'UNAVAILABLE'),
                    '_',
                    COALESCE(enc.admit_dt, 'UNAVAILABLE'),
                    '_',
                    MD5(COALESCE(SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1], 'UNAVAILABLE')),
                    '_',
                    COALESCE(enc.enc_id, 'UNAVAILABLE')
                )
    END                                                                                     AS hv_enc_id,
    CONCAT('150_', COALESCE(prv.claim_id,'UNAVAILABLE'))                                    AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(prv.input_file_name, '/')[SIZE(SPLIT(prv.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	enc.facility_id                                                                         AS vdr_org_id,
	/* hvid */
	CASE
	    WHEN COALESCE(enc.hvid, ptn_pay.hvid) IS NOT NULL
	        THEN COALESCE(enc.hvid, ptn_pay.hvid)
	    WHEN prv_pay.patientid IS NOT NULL
	        THEN CONCAT('150_', prv_pay.patientid)
	    ELSE NULL
	END                                                                                     AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(enc.age, ptn_pay.age),
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(enc.yearofbirth, ptn_pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(enc.age, ptn_pay.age),
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(enc.yearofbirth, ptn_pay.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(enc.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(enc.gender), 1, 1)
	    WHEN SUBSTR(UPPER(ptn_pay.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(ptn_pay.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR
	                            (
	                                COALESCE
	                                    (
	                                        enc.state,
	                                        ptn_pay.state
	                                    ), 1, 2
	                            )))                                                         AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            enc.threedigitzip,
                            ptn_pay.threedigitzip,
                            enc.facility_zip 
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
	/* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
    prv.npi_number                                                                          AS enc_prov_id,
    CASE
        WHEN prv.npi_number IS NULL
          OR prv.physician_role IS NULL
            THEN NULL
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'ADMITTING'
            THEN 'ADMITTING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'ATTENDING'
            THEN 'ATTENDING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'EMERGENCY'
            THEN 'EMERGENCY_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'OPERATING'
            THEN 'OPERATING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 5) = 'OTHER'
            THEN 'OTHER_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 12) = 'PRIMARY CARE'
            THEN 'PRIMARY_CARE_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'REFERRING'
            THEN 'REFERRING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(prv.physician_role), 1, 9) = 'RENDERING'
            THEN 'RENDERING_PROVIDER_NPI'
        ELSE 'OTHER_PROVIDER_NPI'
    END                                                                                     AS enc_prov_id_qual,
   /* data_captr_dt - Added JKS 10/17/2019 */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,    
    'provider'                                                                              AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admit_dt, 1, 7)
	END                                                                                     AS part_mth
 FROM
(
    SELECT
        claim_id,
        npi_number,
        physician_role,
        MAX(input_file_name)        AS input_file_name,
        MAX(hvjoinkey)              AS hvjoinkey
     FROM prv
    /* Eliminate column headers. */
    WHERE UPPER(COALESCE(claim_id, '')) <> 'CLAIM_ID'
      AND npi_number IS NOT NULL
    GROUP BY
        claim_id,
        npi_number,
        physician_role
) prv
 LEFT OUTER JOIN prv_pay
              ON COALESCE(prv.hvjoinkey, 'EMPTY') = COALESCE(prv_pay.hvjoinkey, 'DUMMY')
 LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
              ON COALESCE(prv_pay.patientid, 'EMPTY') = COALESCE(ptn_pay.patientid, 'DUMMY')
 LEFT OUTER JOIN sentry_temp11_claim clm
              ON COALESCE(prv.claim_id, 'EMPTY') = COALESCE(clm.claim_id, 'DUMMY')
 LEFT OUTER JOIN sentry_temp14_parent_child lnk
              ON COALESCE(clm.row_num, 'EMPTY') = COALESCE(lnk.child, 'DUMMY')
 LEFT OUTER JOIN sentry_temp18_encounter enc
              ON COALESCE(lnk.parent, 'EMPTY') = COALESCE(enc.enc_id, 'DUMMY')
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
UNION ALL
-------------------------------------------- Include 340B ID
SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CONCAT
        (
            '150_',
            COALESCE(enc.hvid, 'UNAVAILABLE'),
            '_',
            COALESCE(enc.admit_dt, 'UNAVAILABLE'),
            '_',
            MD5(COALESCE(SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1], 'UNAVAILABLE')),
            '_',
            enc.enc_id
        )                                                                                   AS hv_enc_id,
    CONCAT('150_', COALESCE(enc.claim_id,'UNAVAILABLE'))                                    AS hv_enc_dtl_id,   
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	enc.facility_id                                                                         AS vdr_org_id,
	CASE
	    WHEN enc.hvid      IS NOT NULL THEN enc.hvid
	    WHEN enc.patientid IS NOT NULL THEN CONCAT('150_', enc.patientid)
	    ELSE NULL
	END                                                                                     AS hvid,
	/* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
	    )                                                                                   AS ptnt_age_num,	
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(enc.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(enc.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR(enc.state, 1, 2)))                                     AS ptnt_state_cd,
    MASK_ZIP_CODE(SUBSTR(COALESCE(enc.threedigitzip,enc.facility_zip), 1, 3))               AS ptnt_zip3_cd,	
    /* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,

   enc.340b_id                                                                              AS enc_prov_id,
    CASE
        WHEN enc.340b_id IS NOT NULL THEN '340B_ID'
        ELSE NULL
    END                                                                                     AS enc_prov_id_qual,
   /* data_captr_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,
    'claim'                                                                                 AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admit_dt, 1, 7)
	END                                                                                     AS part_mth
	
FROM sentry_temp18_encounter enc
LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1
WHERE enc.340b_id IS NOT NULL

UNION ALL
-------------------------------------------- Include MEDICARE_PROVIDER_NUMBER
SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CONCAT
        (
            '150_',
            COALESCE(enc.hvid, 'UNAVAILABLE'),
            '_',
            COALESCE(enc.admit_dt, 'UNAVAILABLE'),
            '_',
            MD5(COALESCE(SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1], 'UNAVAILABLE')),
            '_',
            enc.enc_id
        )                                                                                   AS hv_enc_id,
    CONCAT('150_', COALESCE(enc.claim_id,'UNAVAILABLE'))                                    AS hv_enc_dtl_id,   
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	enc.facility_id                                                                         AS vdr_org_id,
	CASE
	    WHEN enc.hvid      IS NOT NULL THEN enc.hvid
	    WHEN enc.patientid IS NOT NULL THEN CONCAT('150_', enc.patientid)
	    ELSE NULL
	END                                                                                     AS hvid,
	/* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
	    )                                                                                   AS ptnt_age_num,	
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(enc.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(enc.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR(enc.state, 1, 2)))                                     AS ptnt_state_cd,
    MASK_ZIP_CODE(SUBSTR(COALESCE(enc.threedigitzip,enc.facility_zip), 1, 3))               AS ptnt_zip3_cd,	
    /* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,

   enc.medicare_provider_number                                                             AS enc_prov_id,
    CASE
        WHEN enc.medicare_provider_number IS NOT NULL THEN 'MEDICARE_PROVIDER_NUMBER'
        ELSE NULL
    END                                                                                     AS enc_prov_id_qual,
   /* data_captr_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,
    'claim'                                                                                 AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admit_dt, 1, 7)
	END                                                                                     AS part_mth
	
FROM sentry_temp18_encounter enc
LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1
WHERE enc.medicare_provider_number IS NOT NULL
