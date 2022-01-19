SELECT
    CURRENT_DATE()                                                              AS crt_dt,
	'07'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_','{VDR_FILE_DT}', '_' ,
        SPLIT(cln.input_file_name, '/')[SIZE(SPLIT(cln.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  hv_enc_id - JKS 2021-12-15
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT('5_', COALESCE(SUBSTR(cln.encounter_date, 1, 10), SUBSTR(cln.date_row_added, 1, 10), '0000-00-00' ),
        '_', cln.practice_key,
        '_', cln.patient_key
    )                                                                           AS hv_enc_id,
    
    cln.practice_key                                                            AS vdr_org_id,
        cln.visit_key                                                               AS vdr_enc_id,
        
        CASE
            WHEN cln.visit_key IS NULL THEN NULL
            ELSE 'VISIT_KEY'
        END                                                                         AS vdr_enc_id_qual,

    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    CAP_AGE(
            VALIDATE_AGE(
            pay.age,
            CAST(
                EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d')
                AS DATE),
            COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
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
        COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                       AS enc_start_dt,
    cln.provider_key                                                            AS enc_rndrg_prov_vdr_id,
    CASE
        WHEN cln.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS enc_rndrg_prov_vdr_id_qual,
    cln.practice_key                                                            AS enc_rndrg_prov_alt_id,
    CASE
        WHEN cln.practice_key IS NULL THEN NULL
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
        SUBSTR(cln.date_row_added, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                       AS data_captr_dt,
    'f_clinical_note'                                                           AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN CAP_DATE
	            (
                    CAST(EXTRACT_DATE(SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 7)
	END                                                                         AS part_mth

FROM f_clinical_note cln  -- (f_clininical_note new file name) 
LEFT OUTER JOIN d_patient ptn           ON COALESCE(cln.patient_key, 'NULL')    = COALESCE(ptn.patient_key, 'empty') 
LEFT OUTER JOIN matching_payload pay    ON COALESCE(ptn.patient_key, 'NULL')    = COALESCE(pay.personid, 'empty') 
LEFT OUTER JOIN d_provider prv          ON COALESCE(cln.provider_key, 'NULL')   = COALESCE(prv.provider_key, 'empty')
WHERE
    TRIM(UPPER(COALESCE(cln.practice_key, 'empty'))) <> 'PRACTICE_KEY'
    
-- LIMIT 100
