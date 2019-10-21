SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('150_', COALESCE(lin.claim_id, 'UNAVAILABLE'))                                   AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'02'                                                                                    AS mdl_vrsn_num,
    SPLIT(lin.input_file_name, '/')[SIZE(SPLIT(lin.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	enc.facility_id                                                                         AS vdr_org_id,
	lin.hvid                                                                                AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(lin.age, clm_ptn_pay.age),
            CAST(EXTRACT_DATE(lin.charge_date, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(lin.yearofbirth, clm_ptn_pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(lin.age, clm_ptn_pay.age),
            CAST(EXTRACT_DATE(lin.charge_date, '%Y-%m-%d') AS DATE),
            SUBSTR(COALESCE(lin.yearofbirth, clm_ptn_pay.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(lin.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(lin.gender), 1, 1)
	    WHEN SUBSTR(UPPER(clm_ptn_pay.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(clm_ptn_pay.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR
	                            (
	                                COALESCE
	                                    (
	                                        lin.state,
	                                        clm_ptn_pay.state
	                                    ), 1, 2
	                            )))                                                         AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            lin.threedigitzip,
                            clm_ptn_pay.threedigitzip,
                            clm.facility_zip 
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
	/* chg_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(lin.charge_date, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS chg_dt,
	CAST(NULL AS DATE)                                                                      AS medctn_admin_dt,
    CLEAN_UP_PROCEDURE_CODE(lin.code)                                                       AS proc_cd,
    /* proc_cd_qual */
    CASE
        WHEN UPPER(SUBSTR(COALESCE(lin.charge_type, ''), 1, 3)) IN ('CPT', 'ICD')
            THEN UPPER(SUBSTR(COALESCE(lin.charge_type, ''), 1, 3))
        WHEN UPPER(SUBSTR(COALESCE(lin.charge_type, ''), 1, 5)) = 'HCPCS'
            THEN 'HC'
        ELSE NULL
    END                                                                                     AS proc_cd_qual,
    CAST(NULL AS STRING)                                                                    AS proc_unit_qty,
    CAST(NULL AS STRING)                                                                    AS medctn_ndc,
    CAST(NULL AS STRING)                                                                    AS medctn_molcl_nm,
    CAST(NULL AS STRING)                                                                    AS medctn_qty,
    CAST(lin.charge_amount AS FLOAT)                                                        AS dtl_chg_amt,
    /* cdm_grp_txt */
    CASE
        WHEN COALESCE
                (
                    lin.charge_type,
                    lin.charge_quantity
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN lin.charge_type IS NULL
                                    THEN ''
                                ELSE CONCAT(' | CHARGE_TYPE: ', lin.charge_type)
                            END,
                            CASE
                                WHEN lin.charge_quantity IS NULL
                                    THEN ''
                                ELSE CONCAT(' | CHARGE_QUANTITY: ', lin.charge_quantity)
                            END
                        ), 4
                )
    END                                                                                     AS cdm_grp_txt,
    loc.loc_grp                                                                             AS cdm_dept_txt,
    /* data_captr_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(clm.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,
    'claim_line'                                                                            AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(lin.charge_date, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(lin.charge_date, 1, 7)
	END                                                                                     AS part_mth
 FROM sentry_temp21_claim_line_dedup lin
 LEFT OUTER JOIN sentry_temp11_claim clm
              ON COALESCE(lin.claim_id, 'EMPTY') = COALESCE(clm.claim_id, 'DUMMY')
 LEFT OUTER JOIN clm_pay
              ON COALESCE(clm.hvjoinkey, 'EMPTY') = COALESCE(clm_pay.hvjoinkey, 'DUMMY')
 LEFT OUTER JOIN sentry_temp00_patient_payload_dedup clm_ptn_pay
              ON COALESCE(clm_pay.patientid, 'EMPTY') = COALESCE(clm_ptn_pay.patientid, 'DUMMY')
 LEFT OUTER JOIN sentry_temp14_parent_child lnk
              ON COALESCE(clm.row_num, 'EMPTY') = COALESCE(lnk.child, 'DUMMY')
 LEFT OUTER JOIN sentry_temp18_encounter enc
              ON COALESCE(lnk.parent, 'EMPTY') = COALESCE(enc.enc_id, 'DUMMY')
 LEFT OUTER JOIN sentry_temp20_location_pivot loc 
              ON COALESCE(lin.claim_id, 'EMPTY') = COALESCE(loc.claim_id, 'DUMMY')
             AND COALESCE(lin.charge_date, 'EMPTY') >= COALESCE(loc.date_time, 'DUMMY')
             AND COALESCE(lin.charge_date, 'EMPTY') < COALESCE(loc.next_loc_dt, 'DUMMY')
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
