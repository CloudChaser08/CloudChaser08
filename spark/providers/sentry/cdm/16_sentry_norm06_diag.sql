SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('150_', COALESCE(dgn.claim_id, 'UNAVAILABLE'))                                   AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(dgn.input_file_name, '/')[SIZE(SPLIT(dgn.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	clm.facility_id                                                                         AS vdr_org_id,
	dgn.claim_id                                                                            AS vdr_diag_id,
	dgn.hvid                                                                                AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(dgn.age, enc.age),
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(dgn.yearofbirth, enc.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(dgn.age, enc.age),
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(dgn.yearofbirth, enc.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(dgn.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(dgn.gender), 1, 1)
	    WHEN SUBSTR(UPPER(enc.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(enc.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR
	                            (
	                                COALESCE
	                                    (
	                                        dgn.state,
	                                        enc.state
	                                    ), 1, 2
	                            )))                                                         AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            dgn.threedigitzip,
                            enc.threedigitzip,
                            enc.facility_zip 
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
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
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        dgn.diagnosis_code,
	        NULL,
	        CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE)
	    )                                                                                   AS diag_cd,
    /* data_captr_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,
    'diagnosis'                                                                             AS prmy_src_tbl_nm,
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
 FROM sentry_temp01_diagnosis_dedup dgn
 LEFT OUTER JOIN sentry_temp11_claim clm
              ON COALESCE(dgn.claim_id, 'EMPTY') = COALESCE(clm.claim_id, 'DUMMY')
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
