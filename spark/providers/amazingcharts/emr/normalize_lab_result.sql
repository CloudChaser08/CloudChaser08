SELECT /*+ BROADCAST (prv1) */ /*+ BROADCAST (prv2) */ /*+ BROADCAST (lbd) */
    CONCAT(
        '5_',
        COALESCE(SUBSTR(lab.sign_off_date, 1, 10), '0000-00-00'),
        '_',
        lab.practice_key,
        '_',
        lab.patient_key
    )                                       AS hv_lab_result_id,
    lab.practice_key                        AS vdr_org_id,
    lab.lab_test_id                         AS vdr_lab_test_id,
    CASE
        WHEN lab.lab_test_id IS NULL THEN NULL
        ELSE 'LAB_TEST_ID'
    END                                     AS vdr_lab_test_id_qual,
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
    UPPER(
        COALESCE(
            ptn.state,
            pay.state
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
    extract_date(
        SUBSTR(lab.specimen_collected_dt, 1, 10),
        '%Y-%m-%d'
    )                                       AS lab_test_smpl_collctn_dt,
    extract_date(
        SUBSTR(lab.sign_off_date, 1, 10),
        '%Y-%m-%d'
    )                                       AS lab_result_dt,
    lab.ordering_provider_id                AS lab_test_ordg_prov_vdr_id,
    CASE
        WHEN lab.ordering_provider_id IS NULL THEN NULL
        ELSE 'SIGN_OFF_ID'
    END                                     AS lab_test_ordg_prov_vdr_id_qual,
    lab.practice_key                        AS lab_test_ordg_prov_alt_id,
    CASE
        WHEN lab.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS lab_test_ordg_prov_alt_id_qual,
    prv1.specialty                          AS lab_test_ordg_prov_alt_speclty_id,
    CASE
        WHEN prv1.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS lab_test_ordg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv1.state, ''))         AS lab_test_ordg_prov_state_cd,
    lab.sign_off_id                         AS lab_test_exectg_fclty_vdr_id,
    CASE
        WHEN lab.sign_off_id IS NULL THEN NULL
        ELSE 'SIGN_OFF_ID'
    END                                     AS lab_test_exectg_fclty_vdr_id_qual,
    prv2.practice_key                       AS lab_test_exectg_fclty_alt_id,
    CASE
        WHEN prv2.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS lab_test_exectg_fclty_alt_id_qual,
    prv2.specialty                          AS lab_test_exectg_fclty_alt_speclty_id,
    CASE
        WHEN prv2.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS lab_test_exectg_fclty_alt_speclty_id_qual,
    UPPER(COALESCE(prv2.state, ''))         AS lab_test_exectg_fclty_state_cd,
    lab.specimen_source                     AS lab_test_specmn_typ_cd,
    CASE
        WHEN lab.fasting = 'True' THEN 'Y'
        WHEN lab.fasting = 'False' THEN 'N'
        ELSE NULL
    END                                     AS lab_test_fstg_stat_flg,
    lbd.test_name                           AS lab_test_nm,
    regexp_replace(
        lab.loinc_test_code,
        '[^0-9]',
        ''
    )                                       AS lab_test_loinc_cd,
    CONCAT(
        lab.practice_key,
        '_',
        lbd.test_code
    )                                       AS lab_test_vdr_cd,
    CASE
        WHEN lab.practice_key IS NULL THEN NULL
        WHEN lbd.test_code IS NULL THEN NULL
        ELSE 'PRACTICE_KEY_TEST_CODE'
    END                                     AS lab_test_vdr_cd_qual,
    lab.observation_value                   AS lab_result_msrmt,
    lab.uom                                 AS lab_result_uom,
    CASE
        WHEN lab.uom IS NULL THEN NULL
        ELSE 'OBSERVATION_VALUE'
    END                                     AS lab_result_qual,
    CASE
        WHEN lab.abnormal_flag = 'N' THEN 'N'
        WHEN lab.abnormal_flag IN ('H', 'L') THEN 'Y'
        ELSE NULL
    END                                     AS lab_result_abnorm_flg,
    CASE
        WHEN INSTR(lab.reference_ranges, '-') IS NOT NULL THEN SPLIT(lab.reference_ranges, '-')[0]
        WHEN SUBSTR(lab.reference_ranges, 1, 1) = '>' THEN lab.reference_ranges
        ELSE NULL
    END                                     AS lab_result_norm_min_msrmt,
    CASE
        WHEN INSTR(lab.reference_ranges, '-') IS NOT NULL THEN SPLIT(lab.reference_ranges, '-')[1]
        WHEN SUBSTR(lab.reference_ranges, 1, 1) = '<' THEN lab.reference_ranges
        ELSE NULL
    END                                     AS lab_result_norm_max_msrmt,
    CASE
        WHEN lab.lab_test_status_lrd = 'F' THEN 'Final'
        WHEN lab.lab_test_status_lrd = 'S' THEN 'Partial'
        WHEN lab.lab_test_status_lrd = 'P' THEN 'Preliminary'
        WHEN lab.lab_test_status_lrd = 'C' THEN 'Correction'
        WHEN lab.lab_test_status_lrd = 'X' THEN 'Cancelled'
        WHEN lab.lab_test_status_lrd = 'I' THEN 'Pending'
        ELSE NULL
    END                                     AS lab_result_stat_cd,
    CASE
        WHEN lab.lab_test_status_lrd NOT IN ('F', 'S', 'P', 'C', 'X', 'I') THEN NULL
        ELSE 'LAB_TEST_STATUS_LRD'
    END                                     AS lab_result_stat_cd_qual,
    extract_date(
        SUBSTR(lab.created_date_lt, 1, 10),
        '%Y-%m-%d'
    )                                       AS data_captr_dt,
    CASE
        WHEN lab.inactive_flag = '1' THEN 'REPLACED'
        ELSE NULL
    END                                     AS rec_stat_cd,
    'f_lab'                                 AS prmy_src_tbl_nm
FROM f_lab lab
LEFT OUTER JOIN d_patient ptn ON lab.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv1 ON lab.ordering_provider_id = prv1.provider_key
LEFT OUTER JOIN d_provider prv2 ON lab.sign_off_id = prv2.provider_key
LEFT OUTER JOIN d_lab_directory lbd ON lab.lab_directory_key = lbd.lab_directory_key AND lab.practice_key = lbd.practice_key
