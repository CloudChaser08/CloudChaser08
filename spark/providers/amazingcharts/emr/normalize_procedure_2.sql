SELECT /*+ BROADCAST (prv) */
    CONCAT(
        '5_',
        SUBSTR(inj.date_given, 1, 10),
        '_',
        inj.practice_key,
        '_',
        inj.patient_key
    )                                       AS hv_proc_id,
    inj.practice_key                        AS vdr_org_id,
    pay.hvid                                AS hvid,
    COALESCE(
        SUBSTR(ptn.birth_date, 1, 4),
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
        SUBSTR(inj.date_given, 1, 10),
        '%Y-%m-%d'
    )                                       AS proc_dt,
    inj.provider_key                        AS proc_rndrg_prov_vdr_id,
    CASE
        WHEN inj.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                     AS proc_rndrg_prov_vdr_id_qual,
    prv.practice_key                        AS proc_rndrg_prov_alt_id,
    CASE
        WHEN prv.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS proc_rndrg_prov_alt_id_qual,
    prv.specialty                           AS proc_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))          AS proc_rndrg_prov_state_cd,
    ARRAY(
        UPPER(inj.cpt),
        UPPER(vcx.cpt_code)
    )[e.n]                                  AS proc_cd,
    ARRAY(
        CASE
            WHEN inj.cpt IS NULL THEN NULL
            ELSE 'F_INJECTION.CPT'
        END,
        CASE
            WHEN vcx.cpt_code IS NULL THEN NULL
            ELSE 'D_VACCINE_CPT.CPT_CODE'
        END
    )[e.n]                                  AS proc_cd_qual,
    regexp_replace(
        inj.volume,
        '[^0-9|\.]',
        ''
    )                                       AS proc_unit_qty,
    regexp_replace(
        inj.volume,
        '[0-9|\.]',
        ''
    )                                       AS proc_uom,
    CASE
        WHEN inj.patient_refused = '1' THEN 'Patient Refused'
        WHEN inj.patient_parent_refused = '1' THEN 'Patient Parent Refused'
        ELSE NULL
    END                                     AS proc_stat_cd,
    CASE
        WHEN inj.patient_refused != '1' OR inj.patient_parent_refused != '1' THEN NULL
        ELSE 'INJECTION_REFUSED'
    END                                     AS proc_stat_cd_qual,
    inj.record_type                         AS proc_typ_cd,
    CASE
        WHEN inj.record_type IS NULL THEN NULL
        ELSE 'RECORD_TYPE'
    END                                     AS proc_typ_cd_qual,
    inj.route                               AS proc_admin_rte_cd,
    inj.site                                AS proc_admin_site_cd,
    inj.deleted                             AS rec_stat_cd,
    'f_injection'                           AS prmy_src_tbl_nm
FROM f_injection inj
    LEFT OUTER JOIN d_patient ptn ON inj.patient_key = ptn.patient_key
    LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
    LEFT OUTER JOIN d_provider prv ON inj.provider_key = prv.provider_key
    LEFT OUTER JOIN d_vaccine_cpt vcx ON inj.vaccine_cpt_key = vcx.vaccine_cpt_key
    INNER JOIN proc_2_exploder e
WHERE
--- Only keep a row if it's proc_cd is not null
    (
        ARRAY(inj.cpt, vcx.cpt_code)[e.n] IS NOT NULL
    )
    OR
--- If inj.cpt and vcx.cpt_code is null, keep one row that has a NULL proc_cd
    (
        COALESCE(inj.cpt, vcx.cpt_code) IS NULL
        AND e.n = 0
    )
