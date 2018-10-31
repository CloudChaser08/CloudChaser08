SELECT /*+ BROADCAST (prv) */
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
    )                                       AS hv_proc_id,
    enc.practice_key                        AS vdr_org_id,
    pay.hvid                                AS hvid,
    COALESCE(
        SUBSTR(ptn.birth_year, 1, 4),
        pay.yearOfBirth
    )                                       AS ptnt_birth_yr,
    pay.age                                 AS ptnt_age_num,
    COALESCE(
        ptn.gender,
        pay.gender
    )                                       AS ptnt_gender_cd,
    COALESCE(
        ptn.state,
        pay.state
    )                                       AS ptnt_state_cd,
    SUBSTR(
        COALESCE(
            ptn.zip,
            pay.threeDigitZip
        ),
        1,
        3
    )                                       AS ptnt_zip3_cd,
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
    )                                       AS hv_enc_id,
    extract_date(
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added
            ),
            1,
            10
        ),
        '%Y-%m-%d'
    )                                       AS enc_dt,
    extract_date(
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added
            ),
            1,
            10
        ),
        '%Y-%m-%d'
    )                                       AS proc_dt,
    enc.provider_key                        AS proc_rndrg_prov_vdr_id,
    CASE
        WHEN enc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                     AS proc_rndrg_prov_vdr_id_qual,
    enc.practice_key                        AS proc_rndrg_prov_alt_id,
    CASE
        WHEN enc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS proc_rndrg_prov_alt_id_qual,
    prv.specialty                           AS proc_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))          AS proc_rndrg_prov_state_cd,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                UPPER(cpt_code),
                '(\\([^)]*\\))|(\\bLOW\\b)|(\\bMEDIUM\\b)|(\\bHIGH\\b)|(\\bCOMPLEXITY\\b)|\\<|\\>',
                ''
            ),
            '[^A-Z0-9]',
            ' '    
        )
    )                                       AS proc_cd,
    CASE
        WHEN enc.cpt_code IS NULL THEN NULL
        ELSE 'CPT_CODE'
    END                                     AS proc_cd_qual,
    extract_date(
        SUBSTR(enc.date_row_added, 1, 10),
        '%Y-%m-%d'
    )                                       AS data_captr_dt,
    'f_encounter'                           AS prmy_src_tbl_nm
FROM f_encounter enc
LEFT OUTER JOIN d_patient ptn ON enc.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON enc.provider_key = prv.provider_key
WHERE LENGTH(TRIM(COALESCE(enc.cpt_code, ''))) != 0
