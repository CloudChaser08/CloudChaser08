SELECT /*+ BROADCAST (prv) */
    CONCAT(
        '5_',
        COALESCE(
            SUBSTR(prc.date_performed, 1, 10),
            '0000-00-00'
        ),
        '_',
        prc.practice_key,
        '_',
        prc.patient_key
    )                                       AS hv_proc_id,
    prc.practice_key                        AS vdr_org_id,
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
    extract_date(
        SUBSTR(prc.date_performed, 1, 10),
        '%Y-%m-%d'
    )                                       AS proc_dt,
    prc.provider_key                        AS proc_rndrg_prov_vdr_id,
    CASE
        WHEN prc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                     AS proc_rndrg_prov_vdr_id_qual,
    prc.practice_key                        AS proc_rndrg_prov_alt_id,
    CASE
        WHEN prc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS proc_rndrg_prov_alt_id_qual,
    prv.specialty                           AS proc_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))          AS proc_rndrg_prov_state_cd,
    TRIM(UPPER(cpt.cpt_code))               AS proc_cd,
    CASE
        WHEN cpt.cpt_code IS NULL THEN NULL
        ELSE 'CPT_CODE'
    END                                     AS proc_cd_qual,
    prc.units                               AS proc_unit_qty,
    'f_procedure'                           AS prmy_src_tbl_nm
FROM f_procedure prc
LEFT OUTER JOIN d_patient ptn ON prc.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON prc.provider_key = prv.provider_key
LEFT OUTER JOIN d_cpt cpt ON prc.cpt_key = cpt.cpt_key
