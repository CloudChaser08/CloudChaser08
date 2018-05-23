SELECT /*+ BROADCAST (prv) */
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                dig.date_active,
                dig.date_row_added
            ),
            1,
            10
        ),
        '_',
        dig.practice_key,
        '_',
        dig.patient_key
    )                                                   AS hv_diag_id,
    dig.practice_key                                    AS vdr_org_id,
    pay.hvid                                            AS hvid,
    COALESCE(
        SUBSTR(
            ptn.birth_date,
            1,
            4
        ),
        pay.yearOfBirth
    )                                                   AS ptnt_birth_yr,
    pay.age                                             AS ptnt_age_num,
    COALESCE(
        ptn.gender,
        pay.gender
    )                                                   AS ptnt_gender_cd,
    UPPER(
        COALESCE(
            ptn.state,
            pay.state,
            ''
        )
    )                                                   AS ptnt_state_cd,
    SUBSTR(
        COALESCE(
            ptn.zip,
            pay.threeDigitZip
        ),
        1,
        3
    )                                                   AS ptnt_zip3_cd,
    extract_date(
        SUBSTR(
            COALESCE(
                dig.date_active,
                dig.date_row_added
            ),
            1,
            10
        ),
        '%Y-%m-%d'
    )                                                   AS diag_dt,
    dig.provider_key                                    AS diag_rndrg_prov_vdr_id,
    CASE
        WHEN dig.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                 AS diag_rndrg_prov_vdr_id_qual,
    prv.practice_key                                    AS diag_rndrg_prov_alt_id,
    CASE
        WHEN prv.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                 AS diag_rndrg_prov_alt_id_qual,
    prv.specialty                                       AS diag_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                 AS diag_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))                      AS diag_rndrg_prov_state_cd,
    extract_date(
        SUBSTR(dig.date_active, 1, 10),
        '%Y-%m-%d'
    )                                                   AS diag_onset_dt,
    extract_date(
        SUBSTR(dig.date_resolved, 1, 10),
        '%Y-%m-%d'
    )                                                   AS diag_resltn_dt,
    UPPER(dig.problem_icd)                              AS diag_cd,
    CASE
        WHEN dig.problem_icd IS NULL THEN NULL
        WHEN dig.icd_type = '9' THEN '01'
        WHEN dig.icd_type = '10' THEN '02'
        ELSE NULL
    END                                                 AS diag_cd_qual,
    UPPER(dig.snomed)                                   AS diag_snomed_cd,
    UPPER(dig.record_type)                              AS data_src_cd,
    extract_date(
        SUBSTR(dig.date_row_added, 1, 10),
        '%Y-%m-%d'
    )                                                   AS data_captr_dt,
    'f_diagnosis'                                       AS prmy_src_tbl_nm
FROM f_diagnosis dig
LEFT OUTER JOIN d_patient ptn ON dig.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON dig.provider_key = prv.provider_key
