SELECT /*+ BROADCAST (prv) */
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added,
                '0000-00-00'
            ),
            1,
            10
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                       AS hv_clin_obsn_id,
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
    UPPER(
        COALESCE(
            ptn.state,
            pay.state,
            ''
        )
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
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added,
                '0000-00-00'
            ),
            1,
            10
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
    )                                       AS clin_obsn_dt,
    enc.provider_key                        AS clin_obsn_rndrg_prov_vdr_id,
    CASE
        WHEN enc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                     AS clin_obsn_rndrg_prov_vdr_id_qual,
    enc.practice_key                        AS clin_obsn_rndrg_prov_alt_id,
    CASE
        WHEN enc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS clin_obsn_rndrg_prov_alt_id_qual,
    prv.specialty                           AS clin_obsn_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS clin_obsn_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))          AS clin_obsn_rndrg_prov_state_cd,
    ARRAY(
        'PARTURITION',
        'LAST_MENSTRUAL_PERIOD',
        'SMOKING',
        'SMOKING',
        'SMOKING'
    )[e.n]                                  AS clin_obsn_cd,
    ARRAY(
        SUBSTR(enc.estimated_delivery_date, 1, 10),
        SUBSTR(enc.last_menstrual_period, 1, 10),
        enc.packs_per_day,
        enc.years_smoked,
        enc.years_quit
    )[e.n]                                  AS clin_obsn_msrmt,
    -- clin_obsn_uom
    ARRAY(
        'ESTIMATED_DATE',
        'DATE',
        'PACKS_PER_DAY',
        'YEARS_SMOKED',
        'YEARS_QUIT'
    )[e.n]                                  AS clin_obsn_uom,
    extract_date(
        SUBSTR(enc.date_row_added, 1, 10),
        '%Y-%m-%d'
    )                                       AS data_captr_dt,
    'f_encounter'                           AS prmy_src_tbl_nm
FROM f_encounter enc
    LEFT OUTER JOIN d_patient ptn ON enc.patient_key = ptn.patient_key
    LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
    LEFT OUTER JOIN d_provider prv ON enc.provider_key = prv.provider_key
    INNER JOIN clin_obsn_exploder e
WHERE
-- Only keep a clin obsn if the measurement is not null --
-- Don't create a target row with NULL code if measurements are all NULL --
    ARRAY(
        enc.estimated_delivery_date,
        enc.last_menstrual_period,
        enc.packs_per_day,
        enc.years_smoked,
        enc.years_quit
    )[e.n] IS NOT NULL
