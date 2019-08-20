SELECT
    /* hv_enc_dtl_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(ptn_chg.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_dtl_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(ptn_chg.input_file_name, '/')[SIZE(SPLIT(ptn_chg.input_file_name, '/')) - 1]      AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	ptn_chg.record_id                                                                       AS vdr_enc_dtl_id,
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
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(epi.discharge_dt, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
	/* proc_dt */
	CASE
	    WHEN CAST(COALESCE(ptn_chg.service_day, 'X') AS INTEGER) IS NULL
	        THEN NULL
        ELSE CAP_DATE
        	    (
        	        DATE_ADD(CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE), CAST(ptn_chg.service_day AS INTEGER)),
                    esdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
	END                                                                                     AS proc_dt,
    CLEAN_UP_PROCEDURE_CODE(ptn_chg.cpt_code)                                               AS proc_cd,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(ptn_chg.cpt_modifier), 1, 2)                          AS proc_cd_1_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_2_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_3_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_seq_cd,
    ptn_chg.units                                                                           AS proc_unit_qty,
    CAST(NULL AS STRING)                                                                    AS proc_grp_txt,
    CAST(ptn_chg.charge AS FLOAT)                                                           AS dtl_chg_amt,
    /* chg_meth_desc */
    CASE
        WHEN COALESCE(cdm.chg_in_time, 'X') = '1'
            THEN 'Time'
        ELSE NULL
    END                                                                                     AS chg_meth_desc,
    CAST(NULL AS STRING)                                                                    AS cdm_grp_txt,
    /* cdm_convsn_txt */
    CASE
        WHEN COALESCE
                (
                    cdm.cdm_to_std_conv_factor,
                    s_cdm.std_to_cpm_conv_factor
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN cdm.cdm_to_std_conv_factor IS NULL
                                    THEN ''
                                ELSE CONCAT(' | CDM_TO_STANDARD_CONVERSION_FACTOR: ', cdm.cdm_to_std_conv_factor)
                            END,
                            CASE
                                WHEN s_cdm.std_to_cpm_conv_factor IS NULL
                                    THEN ''
                                ELSE CONCAT(' | STANDARD_TO_CPM_CONVERSION_FACTOR: ', s_cdm.std_to_cpm_conv_factor)
                            END
                        ), 4
                )
    END                                                                                     AS cdm_convsn_txt,
    /* cdm_dept_txt */
    CASE
        WHEN COALESCE
                (
                    cdm.cdm_dept_code,
                    s_cdm.std_dept_code,
                    s_cdm.std_dept_desc
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN cdm.cdm_dept_code IS NULL
                                    THEN ''
                                ELSE CONCAT(' | DEPARTMENT_CODE: ', cdm.cdm_dept_code)
                            END,
                            CASE
                                WHEN COALESCE(s_cdm.std_dept_code, s_cdm.std_dept_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | STANDARD_DEPARTMENT_CODE: ',
                                            COALESCE(s_cdm.std_dept_code, ''),
                                            ' - ',
                                            COALESCE(s_cdm.std_dept_desc, '')
                                        )
                            END
                        ), 4
                )
    END                                                                                     AS cdm_dept_txt,
    /* std_cdm_grp_txt */
    CASE
        WHEN COALESCE
                (
                    s_cdm.cpt_code,
                    s_cdm.hcpcs_code,
                    s_cdm.hcpcs_modifier
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN s_cdm.cpt_code IS NULL
                                    THEN ''
                                ELSE CONCAT(' | STANDARD_CDM_CPT_CODE: ', s_cdm.cpt_code)
                            END,
                            CASE
                                WHEN s_cdm.hcpcs_code IS NULL
                                    THEN ''
                                ELSE CONCAT(' | STANDARD_CDM_HCPCS_CODE: ', s_cdm.hcpcs_code)
                            END,
                            CASE
                                WHEN s_cdm.hcpcs_modifier IS NULL
                                    THEN ''
                                ELSE CONCAT(' | STANDARD_CDM_HCPCS_MODIFIER: ', s_cdm.hcpcs_modifier)
                            END
                        ), 4
                )
    END                                                                                     AS std_cdm_grp_txt,
    /* vdr_chg_desc */
    CASE
        WHEN COALESCE
                (
                    cdm.charge_code,
                    cdm.charge_desc
                ) IS NULL
            THEN NULL
        ELSE CONCAT
                (
                    'VENDOR_CHARGE_CODE: ',
                    COALESCE(cdm.charge_code, ''),
                    ' - ',
                    COALESCE(cdm.charge_desc, '')
                )
    END                                                                                     AS vdr_chg_desc,
    /* std_chg_desc */
    CASE
        WHEN COALESCE
                (
                    s_cdm.cpm_code,
                    s_cdm.cpm_desc,
                    s_cdm.cdm_std_code,
                    s_cdm.cdm_std_desc
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN COALESCE(s_cdm.cpm_code, s_cdm.cpm_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | CPM_CHARGE_CODE: ',
                                            COALESCE(s_cdm.cpm_code, ''),
                                            ' - ',
                                            COALESCE(s_cdm.cpm_desc, '')
                                        )
                            END,
                            CASE
                                WHEN COALESCE(s_cdm.cdm_std_code, s_cdm.cdm_std_desc) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | STANDARD_CHARGE_CODE: ',
                                            COALESCE(s_cdm.cdm_std_code, ''),
                                            ' - ',
                                            COALESCE(s_cdm.cdm_std_desc, '')
                                        )
                            END
                        ), 4
                )
    END                                                                                     AS std_chg_desc,
    /* cdm_manfctr_txt */
    CASE
        WHEN COALESCE
                (
                    s_cdm.manuf_name,
                    s_cdm.manuf_cat_num,
                    s_cdm.manuf_descr
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN s_cdm.manuf_name IS NULL
                                    THEN ''
                                ELSE CONCAT(' | MANUFACTURER_NAME: ', s_cdm.manuf_name)
                            END,
                            CASE
                                WHEN COALESCE(s_cdm.manuf_cat_num, s_cdm.manuf_descr) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | MANUFACTURER_ITEM: ',
                                            COALESCE(s_cdm.manuf_cat_num, ''),
                                            ' - ',
                                            COALESCE(s_cdm.manuf_descr, '')
                                        )
                            END
                        ), 4
                )
    END                                                                                     AS cdm_manfctr_txt,
    'patient_charges'                                                                       AS prmy_src_tbl_nm,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(epi.admit_dt, '%Y%m%d') AS DATE),
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(epi.admit_dt, 1, 4), '-',
                    SUBSTR(epi.admit_dt, 5, 2)
                )
	END                                                                                     AS part_mth
 FROM patient_charges ptn_chg
 LEFT OUTER JOIN chargemaster cdm
   ON COALESCE(ptn_chg.charge_id, 'EMPTY') = COALESCE(cdm.charge_id, 'DUMMY')
 LEFT OUTER JOIN standard_chargemaster s_cdm
   ON COALESCE(cdm.cdm_std_id, 'EMPTY') = COALESCE(s_cdm.cdm_std_id, 'DUMMY')
 LEFT OUTER JOIN episodes epi
   ON COALESCE(ptn_chg.record_id, 'EMPTY') = COALESCE(epi.record_id, 'DUMMY')
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
WHERE UPPER(COALESCE(ptn_chg.record_id, '')) <> 'RECORD_ID'