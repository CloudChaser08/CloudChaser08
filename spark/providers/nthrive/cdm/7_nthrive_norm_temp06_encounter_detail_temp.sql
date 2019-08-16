SELECT
    /* hv_enc_dtl_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(ptn_prc.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(ptn_prc.input_file_name, '/')[SIZE(SPLIT(ptn_prc.input_file_name, '/')) - 1]      AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	ptn_prc.record_id                                                                       AS vdr_enc_dtl_id,
    /* hvid */
    COALESCE
        (
            pay.hvid, 
            CONCAT
                (
                    '149_', 
                    COALESCE
                        (
                            epi.unique_patient_id, 
                            ptn.unique_patient_id,
                            'UNAVAILABLE'
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
            esdt.gen_ref_1_dt,
            CAST(${VDR_FILE_DT} AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(epi.discharge_dt, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST(${VDR_FILE_DT} AS DATE)
	    )                                                                                   AS enc_end_dt,
	/* proc_dt */
	CASE
	    WHEN CAST(COALESCE(ptn_prc.procedure_day, 'X') AS INTEGER) IS NULL
	        THEN NULL
        ELSE CAP_DATE
        	    (
        	        DATE_ADD(CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE), CAST(ptn_prc.procedure_day AS INTEGER)),
                    esdt.gen_ref_1_dt,
                    CAST(${VDR_FILE_DT} AS DATE)
        	    )
	END                                                                                     AS proc_dt,
    CLEAN_UP_PROCEDURE_CODE(ptn_prc.icd_procedure_code)                                     AS proc_cd,
    CAST(NULL AS STRING)                                                                    AS proc_cd_1_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_2_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_3_modfr,
    ptn_prc.procedure_order                                                                 AS proc_seq_cd,
    CAST(NULL AS STRING)                                                                    AS proc_unit_qty,
    /* proc_grp_txt */
    CASE
        WHEN COALESCE
                (
                    ptn_prc.proc_class,
                    ptn_prc.proc_class_desc,
                    ptn_prc.ahrq_version
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN COALESCE(ptn_prc.proc_class, ptn_prc.proc_class_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PROCEDURE_CLASS: ', 
                                            COALESCE(ptn_prc.proc_class, ''), 
                                            ' - ', 
                                            COALESCE(ptn_prc.proc_class_desc, '')
                                        )
                            END,
                            CASE
                                WHEN ptn_prc.ahrq_version IS NULL
                                    THEN ''
                                ELSE CONCAT(' | AHRQ_VERSION: ', ptn_prc.ahrq_version)
                            END
                        ), 4
                )
    END                                                                                     AS proc_grp_txt,
    CAST(NULL AS FLOAT)                                                                     AS dtl_chg_amt,
    CAST(NULL AS STRING)                                                                    AS cdm_grp_txt,
    CAST(NULL AS STRING)                                                                    AS cdm_dept_txt,
    CAST(NULL AS STRING)                                                                    AS std_cdm_grp_txt,
    CAST(NULL AS STRING)                                                                    AS vdr_chg_desc,
    CAST(NULL AS STRING)                                                                    AS std_chg_desc,
    CAST(NULL AS STRING)                                                                    AS cdm_manfctr_txt,
    'patient_procedure'                                                                     AS prmy_src_tbl_nm,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(${VDR_FILE_DT} AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(epi.admit_dt, 1, 4), '-',
                    SUBSTR(epi.admit_dt, 5, 2)
                )
	END                                                                                     AS part_mth
 FROM patient_procedure ptn_prc
 LEFT OUTER JOIN episodes epi
   ON COALESCE(ptn_prc.record_id, 'EMPTY') = COALESCE(epi.record_id, 'DUMMY')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn.record_id, 'DUMMY')
 LEFT OUTER JOIN matching_payload pay
   ON COALESCE(ptn.hvjoinkey, 'EMPTY') = COALESCE(pay.hvjoinkey, 'DUMMY')
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 149
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 149
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
/* Eliminate column headers. */
WHERE UPPER(COALESCE(ptn_prc.record_id, '')) <> 'RECORD_ID'
/* Only load records that haven't already been loaded from patient_charges. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM nthrive_norm_temp03_encounter_detail_temp ptn_chg
        WHERE COALESCE(ptn_prc.record_id, 'DUMMY') = ptn_chg.record_id
          AND COALESCE(ptn_prc.procedure_day, 'DUMMY') = ptn_chg.service_day
          AND COALESCE(ptn_prc.icd_procedure_code, 'DUMMY') = ptn_chg.cpt_code
    )
