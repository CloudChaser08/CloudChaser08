SELECT
    CURRENT_DATE()                                                              AS crt_dt,
	'07'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_','{VDR_FILE_DT}', '_' ,
        SPLIT(lab.input_file_name, '/')[SIZE(SPLIT(lab.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    CONCAT(
        '5_',
        COALESCE(SUBSTR(lab.sign_off_date, 1, 10), '0000-00-00'),
        '_',
        lab.practice_key,
        '_',
        lab.patient_key
    )                                                                           AS hv_lab_result_id,
    lab.practice_key                                                            AS vdr_org_id,
--    lab.lab_test_id                                                             AS vdr_lab_test_id,
    lab.lab_test_key                                                            AS vdr_lab_test_id,
    CASE
--        WHEN lab.lab_test_id IS NULL THEN NULL
        WHEN lab.lab_test_key IS NULL THEN NULL
        ELSE 'LAB_TEST_ID'
    END                                                                         AS vdr_lab_test_id_qual,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(lab.specimen_collected_dt, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(lab.specimen_collected_dt, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_age_num,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTR(lab.specimen_collected_dt, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS lab_test_smpl_collctn_dt,
    EXTRACT_DATE(
        SUBSTR(lab.sign_off_date, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS lab_result_dt,
    lab.ordering_provider_id                                                    AS lab_test_ordg_prov_vdr_id,
    CASE
        WHEN lab.ordering_provider_id IS NULL THEN NULL
        ELSE 'SIGN_OFF_ID'
    END                                                                         AS lab_test_ordg_prov_vdr_id_qual,
    lab.practice_key                                                            AS lab_test_ordg_prov_alt_id,
    CASE
        WHEN lab.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS lab_test_ordg_prov_alt_id_qual,
    prv1.specialty                                                              AS lab_test_ordg_prov_alt_speclty_id,
    CASE
        WHEN prv1.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS lab_test_ordg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(prv1.state, ''))
        )                                                                       AS lab_test_ordg_prov_state_cd,
    lab.sign_off_id                                                             AS lab_test_exectg_fclty_vdr_id,
    CASE
        WHEN lab.sign_off_id IS NULL THEN NULL
        ELSE 'SIGN_OFF_ID'
    END                                                                         AS lab_test_exectg_fclty_vdr_id_qual,
    prv2.practice_key                                                           AS lab_test_exectg_fclty_alt_id,
    CASE
        WHEN prv2.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS lab_test_exectg_fclty_alt_id_qual,
    prv2.specialty                                                              AS lab_test_exectg_fclty_alt_speclty_id,
    CASE
        WHEN prv2.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS lab_test_exectg_fclty_alt_speclty_id_qual,
    UPPER(COALESCE(prv2.state, ''))                                             AS lab_test_exectg_fclty_state_cd,
    lab.specimen_source                                                         AS lab_test_specmn_typ_cd,
    CASE
        WHEN lab.fasting = 'True' OR lab.fasting = '1' THEN 'Y'
        WHEN lab.fasting = 'False' OR lab.fasting = '0' THEN 'N'
        ELSE NULL
    END                                                                         AS lab_test_fstg_stat_flg,
    lbd.test_name                                                               AS lab_test_nm,
    CLEAN_UP_LOINC_CODE(
        regexp_replace(
            lab.loinc_test_code,
            '[^0-9]',
            ''
        )
    )                                                                           AS lab_test_loinc_cd,
    CONCAT(
        lab.practice_key,
        '_',
        lbd.test_code
    )                                                                           AS lab_test_vdr_cd,
    CASE
        WHEN lab.practice_key IS NULL THEN NULL
        WHEN lbd.test_code IS NULL THEN NULL
        ELSE 'PRACTICE_KEY_TEST_CODE'
    END                                                                         AS lab_test_vdr_cd_qual,
    lbd.test_name                                                              AS lab_result_nm,
    lab.observation_value                                                       AS lab_result_msrmt,
    lab.uom                                                                     AS lab_result_uom,
    CASE
        WHEN lab.uom IS NULL THEN NULL
        ELSE 'OBSERVATION_VALUE'
    END                                                                         AS lab_result_qual,
    CASE
        WHEN lab.abnormal_flag = 'N' THEN 'N'
        WHEN lab.abnormal_flag IN ('H', 'L') THEN 'Y'
        ELSE NULL
    END                                                                         AS lab_result_abnorm_flg,
    CASE
        WHEN INSTR(lab.reference_ranges, '-') IS NOT NULL THEN SPLIT(lab.reference_ranges, '-')[0]
        WHEN SUBSTR(lab.reference_ranges, 1, 1) = '>' THEN lab.reference_ranges
        ELSE NULL
    END                                                                         AS lab_result_norm_min_msrmt,
    CASE
        WHEN INSTR(lab.reference_ranges, '-') IS NOT NULL THEN SPLIT(lab.reference_ranges, '-')[1]
        WHEN SUBSTR(lab.reference_ranges, 1, 1) = '<' THEN lab.reference_ranges
        ELSE NULL
    END                                                                         AS lab_result_norm_max_msrmt,
    CASE
        WHEN lab.lab_test_status_lrd = 'F' THEN 'Final'
        WHEN lab.lab_test_status_lrd = 'S' THEN 'Partial'
        WHEN lab.lab_test_status_lrd = 'P' THEN 'Preliminary'
        WHEN lab.lab_test_status_lrd = 'C' THEN 'Correction'
        WHEN lab.lab_test_status_lrd = 'X' THEN 'Cancelled'
        WHEN lab.lab_test_status_lrd = 'I' THEN 'Pending'
        ELSE NULL
    END                                                                         AS lab_result_stat_cd,
    CASE
        WHEN lab.lab_test_status_lrd NOT IN ('F', 'S', 'P', 'C', 'X', 'I') THEN NULL
        ELSE 'LAB_TEST_STATUS_LRD'
    END                                                                         AS lab_result_stat_cd_qual,
    EXTRACT_DATE(
        SUBSTR(lab.created_date_lt, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)

    )                                                                           AS data_captr_dt,
    CASE
        WHEN lab.inactive_flag = '1' THEN 'REPLACED'
        ELSE NULL
    END                                                                         AS rec_stat_cd,
    'f_lab'                                                                     AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN CAP_DATE
	            (
                    CAST(EXTRACT_DATE(SUBSTR(lab.sign_off_date, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(lab.sign_off_date, 1, 7)
	END                                                                         AS part_mth

FROM f_lab lab
LEFT OUTER JOIN d_patient ptn ON lab.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv1 ON lab.ordering_provider_id = prv1.provider_key
LEFT OUTER JOIN d_provider prv2 ON lab.sign_off_id = prv2.provider_key
LEFT OUTER JOIN d_lab_directory lbd ON lab.lab_directory_key = lbd.lab_directory_key -- AND lab.practice_key = lbd.practice_key
--LEFT OUTER JOIN d_lab_directory lbd2 ON lab.lab_test_code_lrd = lbd2.test_code AND lbd.lab_company = lbd2.lab_company AND lab.practice_key = lbd2.practice_key
WHERE
    TRIM(UPPER(COALESCE(lab.practice_key, 'empty'))) <> 'PRACTICE_KEY'
-- LIMIT 100
