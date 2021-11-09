SELECT
    MONOTONICALLY_INCREASING_ID()                                               AS row_id,
    CURRENT_DATE()                                                              AS crt_dt,
	'10'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_{VDR_FILE_DT}_',
        SPLIT(dig.input_file_name, '/')[SIZE(SPLIT(dig.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                dig.date_active,
                dig.date_row_added,
                '0000-00-00'
            ),
            1,
            10
        ),
        '_',
        dig.practice_key,
        '_',
        dig.patient_key
    )                                                                           AS hv_diag_id,
    dig.practice_key                                                            AS vdr_org_id,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(dig.date_active, SUBSTR(dig.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(dig.date_active, SUBSTR(dig.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_age_num,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTR(COALESCE(dig.date_active, dig.date_row_added), 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS diag_dt,
    dig.provider_key                                                            AS diag_rndrg_prov_vdr_id,
    CASE
        WHEN dig.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS diag_rndrg_prov_vdr_id_qual,
    prv.practice_key                                                            AS diag_rndrg_prov_alt_id,
    CASE
        WHEN prv.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS diag_rndrg_prov_alt_id_qual,
    prv.specialty                                                               AS diag_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS diag_rndrg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(prv.state, ''))
    )                                                                           AS diag_rndrg_prov_state_cd,
    EXTRACT_DATE(
        SUBSTR(dig.date_active, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS diag_onset_dt,
    EXTRACT_DATE(
        SUBSTR(dig.date_resolved, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS diag_resltn_dt,
    CLEAN_UP_DIAGNOSIS_CODE(
        UPPER(dig.problem_icd),
        (
            CASE
                WHEN dig.problem_icd IS NULL THEN NULL
                WHEN dig.icd_type = '9' THEN '01'
                WHEN dig.icd_type = '10' THEN '02'
                ELSE NULL
            END
        ),
        EXTRACT_DATE(SUBSTR(COALESCE(dig.date_active, dig.date_row_added), 1, 10), '%Y-%m-%d')
    )                                                                           AS diag_cd,
    CASE
        WHEN dig.problem_icd IS NULL THEN NULL
        WHEN dig.icd_type = '9' THEN '01'
        WHEN dig.icd_type = '10' THEN '02'
        ELSE NULL
    END                                                                         AS diag_cd_qual,
    UPPER(dig.snomed)                                                           AS diag_snomed_cd,
    UPPER(dig.record_type)                                                      AS data_src_cd,
    EXTRACT_DATE(
        SUBSTR(dig.date_row_added, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)

    )                                                                           AS data_captr_dt,
    'f_diagnosis'                                                               AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN 0 = LENGTH(
	    COALESCE(
            CAP_DATE(
                CAST(EXTRACT_DATE(COALESCE(dig.date_active, SUBSTR(dig.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE),
                CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
                ),
                ''
            )
        ) THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(dig.date_active, SUBSTR(dig.date_row_added, 1, 10)), 1, 7)
	END                                                                         AS part_mth
FROM f_diagnosis dig
LEFT OUTER JOIN d_patient ptn ON dig.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON dig.provider_key = prv.provider_key
LEFT OUTER JOIN gen_ref_whtlst ref ON
    UPPER(dig.snomed) = UPPER(ref.gen_ref_nm) AND ref.gen_ref_domn_nm = 'SNOMED' AND ref.gen_ref_whtlst_flg ='Y'
WHERE
    TRIM(UPPER(COALESCE(dig.practice_key, 'empty'))) <> 'PRACTICE_KEY'
