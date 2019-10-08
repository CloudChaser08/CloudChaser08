SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('150_', COALESCE(dsp.dispensation_id, 'UNAVAILABLE'))                            AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'02'                                                                                    AS mdl_vrsn_num,
    SPLIT(dsp.input_file_name, '/')[SIZE(SPLIT(dsp.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	dsp.dispensation_id                                                                     AS vdr_enc_dtl_id,
	dsp.hvid                                                                                AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            dsp.age,
            CAST(EXTRACT_DATE(SUBSTR(dsp.service_date, 1, 10), '%Y-%m-%d') AS DATE),
            SUBSTR(dsp.yearofbirth, 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            dsp.age,
            CAST(EXTRACT_DATE(SUBSTR(dsp.service_date, 1, 10), '%Y-%m-%d') AS DATE), 
            SUBSTR(dsp.yearofbirth, 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(dsp.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(dsp.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR(dsp.state, 1, 2)))                                     AS ptnt_state_cd,
	MASK_ZIP_CODE(SUBSTR(dsp.threedigitzip, 1, 3))                                          AS ptnt_zip3_cd,
	CAST(NULL AS DATE)                                                                      AS enc_start_dt,
	CAST(NULL AS DATE)                                                                      AS enc_end_dt,
	CAST(NULL AS DATE)                                                                      AS chg_dt,
	/* medctn_admin_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(dsp.service_date, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS medctn_admin_dt,
    CLEAN_UP_PROCEDURE_CODE(dsp.hcpcs_code)                                                 AS proc_cd,
    /* proc_cd_qual */
    CASE
        WHEN dsp.hcpcs_code IS NOT NULL
            THEN 'HC'
        ELSE NULL
    END                                                                                     AS proc_cd_qual,
    dsp.hcpcs_quantity                                                                      AS proc_unit_qty,
    CLEAN_UP_NDC_CODE(dsp.ndc)                                                              AS medctn_ndc,
    dsp.molecule                                                                            AS medctn_molcl_nm,
    dsp.dispensation_quantity                                                               AS medctn_qty,
    CAST(dsp.charge_amount AS FLOAT)                                                        AS dtl_chg_amt,
    CAST(NULL AS STRING)                                                                    AS cdm_grp_txt,
    CAST(NULL AS STRING)                                                                    AS cdm_dept_txt,
    CAST(NULL AS DATE)                                                                      AS data_captr_dt,
    'dispensation'                                                                          AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(SUBSTR(dsp.service_date, 1, 10), '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(dsp.service_date, 1, 7)
	END                                                                                     AS part_mth
 FROM sentry_temp02_dispensation_dedup dsp
 LEFT OUTER JOIN dsp_pay
              ON COALESCE(dsp.hvjoinkey, 'EMPTY') = COALESCE(dsp_pay.hvjoinkey, 'DUMMY')
 LEFT OUTER JOIN sentry_temp00_patient_payload_dedup ptn_pay
              ON COALESCE(dsp_pay.patientid, 'EMPTY') = COALESCE(ptn_pay.patientid, 'DUMMY')
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
