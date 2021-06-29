SELECT 
    /* hv_enc_dtl_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(ptn_cpt.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'04'                                                                                    AS mdl_vrsn_num,
    SPLIT(ptn_cpt.input_file_name, '/')[SIZE(SPLIT(ptn_cpt.input_file_name, '/')) - 1]      AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	ptn_cpt.record_id                                                                       AS vdr_enc_dtl_id,
    /* hvid */
    COALESCE
        (
            pay.hvid, 
            CONCAT
                (
                    '492_', 
                    COALESCE
                        (
                            epi.unique_patient_id, 
                            ptn.unique_patient_id
                        )
                )
        )                                                                                   AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(epi.age, pay.age),
            CAST(EXTRACT_DATE(epi.discharge_dt, '%Y%m%d') AS DATE),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(epi.age, pay.age),
            CAST(EXTRACT_DATE(epi.discharge_dt, '%Y%m%d') AS DATE),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(epi.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(epi.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(RIGHT(COALESCE(ptn.patientstate, pay.state), 2))                    AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            epi.zip_code,
                            ptn.patientzipcode,
                            pay.threedigitzip,
                            ptn.facilityzip
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
    /* hv_enc_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(epi.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
	/* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(epi.discharge_dt, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,

	/* Change in Logic for proc_dt population 2020-08-14 */
    CAP_DATE
    (
	CASE
	    WHEN CAST(COALESCE(ptn_cpt.procedure_day, 'X') AS INTEGER) IS NULL                           THEN NULL
	    WHEN CAST(ptn_cpt.procedure_day AS INTEGER) = 0 OR CAST(ptn_cpt.procedure_day AS INTEGER) = 1  THEN TO_DATE(epi.admit_dt, 'yyyyMMdd') 
	    WHEN CAST(ptn_cpt.procedure_day AS INTEGER) > 1                                              THEN DATE_ADD(TO_DATE(epi.admit_dt, 'yyyyMMdd'), CAST(ptn_cpt.procedure_day AS INTEGER)-1)
	    WHEN CAST(ptn_cpt.procedure_day AS INTEGER) < 1                                              THEN DATE_ADD(TO_DATE(epi.admit_dt, 'yyyyMMdd'), CAST(ptn_cpt.procedure_day AS INTEGER))
	ELSE NULL
	END,
	CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
    CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                       AS proc_dt,	

	
    CLEAN_UP_PROCEDURE_CODE(ptn_cpt.cpt_code)                                               AS proc_cd,
    ptn_cpt.cpt_modifier1                                                                   AS proc_cd_1_modfr,
    ptn_cpt.cpt_modifier2                                                                   AS proc_cd_2_modfr,
    ptn_cpt.cpt_modifier3                                                                   AS proc_cd_3_modfr,
    ptn_cpt.procedure_order                                                                 AS proc_seq_cd,
    CAST(NULL AS STRING)                                                                    AS proc_unit_qty,
    CAST(NULL AS STRING)                                                                    AS proc_grp_txt,
    CAST(NULL AS FLOAT)                                                                     AS dtl_chg_amt,
    CAST(NULL AS STRING)                                                                    AS chg_meth_desc,
    CAST(NULL AS STRING)                                                                    AS cdm_grp_txt,
    CAST(NULL AS STRING)                                                                    AS cdm_conv_txt,
    CAST(NULL AS STRING)                                                                    AS cdm_dept_txt,
    CAST(NULL AS STRING)                                                                    AS std_cdm_grp_txt,
    CAST(NULL AS STRING)                                                                    AS vdr_chg_desc,
    CAST(NULL AS STRING)                                                                    AS std_chg_desc,
    CAST(NULL AS STRING)                                                                    AS cdm_manfctr_txt,
    'patient_cpt'                                                                           AS prmy_src_tbl_nm,
    CAST(NULL AS STRING)                                                                    AS icu_ccu_flg,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE), 
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(epi.admit_dt, 1, 4), '-',
                    SUBSTR(epi.admit_dt, 5, 2)
                )
	END                                                                                     AS part_mth
 FROM nthrive_patient_cpt ptn_cpt
 LEFT OUTER JOIN nthrive_episodes epi
   ON COALESCE(ptn_cpt.record_id, 'EMPTY') = COALESCE(epi.record_id, 'DUMMY')
 LEFT OUTER JOIN nthrive_patient ptn
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn.record_id, 'DUMMY')
 LEFT OUTER JOIN matching_payload pay
   ON COALESCE(ptn.hvjoinkey, 'EMPTY') = COALESCE(pay.hvjoinkey, 'DUMMY')
/* Eliminate column headers. */
WHERE UPPER(COALESCE(ptn_cpt.record_id, '')) <> 'RECORD_ID'
/* Only load records that haven't already been loaded from patient_charges... */
  AND NOT EXISTS
    (
        SELECT 1
         FROM nthrive_norm_temp03_ptn_chg_temp ptn_chg
         WHERE COALESCE(ptn_cpt.record_id, 'DUMMY') = ptn_chg.record_id
          AND COALESCE(ptn_cpt.procedure_day, 'DUMMY') = ptn_chg.service_day
          AND COALESCE(ptn_cpt.cpt_code, 'DUMMY') = ptn_chg.cpt_code
    )
/* ...AND haven't already been loaded from patient_procedure. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM nthrive_norm_temp04_ptn_prc_temp ptn_prc
         WHERE COALESCE(ptn_cpt.record_id, 'DUMMY') = ptn_prc.record_id
          AND COALESCE(ptn_cpt.procedure_day, 'DUMMY') = ptn_prc.procedure_day
          AND COALESCE(ptn_cpt.cpt_code, 'DUMMY') = ptn_prc.icd_procedure_code
    )