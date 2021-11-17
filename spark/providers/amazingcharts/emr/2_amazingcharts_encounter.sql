SELECT
    MONOTONICALLY_INCREASING_ID()                                               AS row_id,
    CURRENT_DATE()                                                              AS crt_dt,
	'10'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_{VDR_FILE_DT}_',
        SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    CONCAT(
        '5_',
        COALESCE(
            SUBSTR(enc.encounter_date, 1, 10),
            SUBSTR(enc.date_row_added, 1, 10),
            '0000-00-00'
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                                                           AS hv_enc_id,
    enc.practice_key                                                            AS vdr_org_id,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    CAP_AGE(
            VALIDATE_AGE(
            pay.age,
            CAST(
                EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d')
                AS DATE),
            COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
        )
    )                                                                           AS ptnt_age_num,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    EXTRACT_DATE(
        COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                       AS enc_start_dt,
    enc.provider_key                                                            AS enc_rndrg_prov_vdr_id,
    CASE
        WHEN enc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS enc_rndrg_prov_vdr_id_qual,
    enc.practice_key                                                            AS enc_rndrg_prov_alt_id,
    CASE
        WHEN enc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS enc_rndrg_prov_alt_id_qual,
    prv.specialty                                                               AS enc_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS enc_rndrg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(prv.state, ''))
        )                                                                       AS enc_rndrg_prov_state_cd,
    EXTRACT_DATE(
        SUBSTR(enc.date_row_added, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                       AS data_captr_dt,
    'f_encounter'                                                               AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN 0 = LENGTH(
	    COALESCE(
            CAP_DATE(
                CAST(EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE),
                CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
                ),
                ''
            )
        ) THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), 1, 7)
	END                                                                         AS part_mth
FROM f_encounter enc
LEFT OUTER JOIN d_patient ptn ON enc.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON enc.provider_key = prv.provider_key
WHERE
    TRIM(UPPER(COALESCE(enc.practice_key, 'empty'))) <> 'PRACTICE_KEY'