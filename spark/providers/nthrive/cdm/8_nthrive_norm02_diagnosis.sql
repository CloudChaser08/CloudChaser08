SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_diag_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(ptn_dgn.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'02'                                                                                    AS mdl_vrsn_num,
    SPLIT(ptn_dgn.input_file_name, '/')[SIZE(SPLIT(ptn_dgn.input_file_name, '/')) - 1]      AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	ptn_dgn.record_id                                                                       AS vdr_diag_id,
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
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(epi.age, pay.age),
            to_date(epi.discharge_dt, 'yyyyMMdd'),
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
            to_date(epi.admit_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
	/* diag_cd */
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        ptn_dgn.icd_diagnosis_code,
	        NULL,
	        epi.discharge_dt
	    )                                                                                   AS diag_cd,
    /* diag_prty_cd */
    ptn_dgn.diagnosis_order                                                                 AS diag_prty_cd,
    COALESCE(ptn_dgn_adm.admit_diag_flg, 'N')                                               AS admtg_diag_flg,
    /* prmy_diag_flg */
    CASE
        WHEN COALESCE(ptn_dgn.code_type, 'X') = 'P'
            THEN 'Y'
        ELSE 'N'
    END                                                                                     AS prmy_diag_flg,
    /* diag_grp_txt */
    CASE
        WHEN COALESCE
                (
                    ptn_dgn.icd_diag_cc, 
                    ptn_dgn.icd_diag_mcc, 
                    ptn_dgn.chronic_condition_ind,
                    ptn_dgn.ahrq_version,
                    ptn_dgn.body_system,
                    ptn_dgn.body_system_desc
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN ptn_dgn.icd_diag_cc IS NULL
                                    THEN ''
                                ELSE
                                    CASE
                                        WHEN COALESCE(ptn_dgn.icd_diag_cc, 'X') = '1'
                                            THEN ' | CMS_COMPLICATING_CONDITION: Yes'
                                        WHEN COALESCE(ptn_dgn.icd_diag_cc, 'X') = '0'
                                            THEN ' | CMS_COMPLICATING_CONDITION: No'
                                        ELSE ''
                                    END
                            END,
                            CASE
                                WHEN ptn_dgn.icd_diag_mcc IS NULL
                                    THEN ''
                                ELSE
                                    CASE
                                        WHEN COALESCE(ptn_dgn.icd_diag_mcc, 'X') = '1'
                                            THEN ' | CMS_MAJOR_COMPLICATING_CONDITION: Yes'
                                        WHEN COALESCE(ptn_dgn.icd_diag_mcc, 'X') = '0'
                                            THEN ' | CMS_MAJOR_COMPLICATING_CONDITION: No'
                                        ELSE ''
                                    END
                            END,
                            CASE
                                WHEN ptn_dgn.chronic_condition_ind IS NULL
                                    THEN ''
                                ELSE
                                    CASE
                                        WHEN COALESCE(ptn_dgn.chronic_condition_ind, 'X') = '1'
                                            THEN ' | AHRQ_COMPLICATING_CONDITION: Yes'
                                        WHEN COALESCE(ptn_dgn.chronic_condition_ind, 'X') = '0'
                                            THEN ' | AHRQ_COMPLICATING_CONDITION: No'
                                        ELSE ''
                                    END
                            END,
                            CASE
                                WHEN ptn_dgn.ahrq_version IS NULL
                                    THEN ''
                                ELSE CONCAT(' | AHRQ_VERSION: ', ptn_dgn.ahrq_version)
                            END,
                            CASE
                                WHEN COALESCE(ptn_dgn.body_system, ptn_dgn.body_system_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | BODY_SYSTEM: ', 
                                            COALESCE(ptn_dgn.body_system, ''), 
                                            ' - ', 
                                            COALESCE(ptn_dgn.body_system_desc, '')
                                        )
                            END
                        ), 4
                )
	END			                                                                            AS diag_grp_txt,
    /*------------------------------ column added(JKS 2020-08-13) -----------------------------*/
	CASE 
	    WHEN UPPER(COALESCE(ptn_dgn.present_on_admit, 'X')) IN ('Y', 'N', 'U', 'W') THEN UPPER(ptn_dgn.present_on_admit)
	ELSE NULL
	END                                                                                     AS prsnt_on_admsn_cd,
    /*------------------------------ -----------------------------------------------------------*/
    'patient_diagnosis'                                                                     AS prmy_src_tbl_nm,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            to_date(epi.admit_dt, 'yyyyMMdd'),
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
 FROM nthrive_norm_temp01_diag_temp ptn_dgn
/* Join to the admit diagnosis flag. */
LEFT OUTER JOIN nthrive_norm_temp02_diag_temp ptn_dgn_adm ON COALESCE(ptn_dgn.record_id, 'EMPTY') = COALESCE(ptn_dgn_adm.record_id, 'DUMMY') AND COALESCE(ptn_dgn.icd_diagnosis_code, 'EMPTY') = COALESCE(ptn_dgn_adm.icd_diagnosis_code, 'DUMMY')
LEFT OUTER JOIN episodes epi                            ON ptn_dgn.record_id = epi.record_id
LEFT OUTER JOIN patient ptn                             ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn.record_id, 'EMPTY')
LEFT OUTER JOIN matching_payload pay                             ON COALESCE(ptn.hvjoinkey, 'EMPTY') = COALESCE(pay.hvjoinkey, 'EMPTY')
/* Eliminate column headers. */
WHERE UPPER(COALESCE(ptn_dgn.record_id, '')) <> 'RECORD_ID'
