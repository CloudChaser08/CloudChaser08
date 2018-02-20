SELECT
    CONCAT('40_', lab.id)                                       AS hv_lab_result_id,
    lab.test_id                                                 AS vdr_lab_test_id,
    CASE
      WHEN lab.test_id IS NOT NULL
      THEN 'SOURCE_SYSTEM_ROW_ID'
    END                                                         AS vdr_lab_test_id_qual,
    lab.id                                                      AS vdr_lab_result_id,
    CASE
      WHEN lab.id IS NOT NULL
      THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_lab_result_id_qual,
    dem.patient_id                                              AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(lab.test_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                         AS ptnt_age_num,
    COALESCE(pay.gender, dem.gender)                            AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                              AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))  AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTRING(lab.test_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS lab_test_execd_dt,
    lab.physician_id                                            AS lab_test_ordg_prov_vdr_id,
    CASE
      WHEN lab.physician_id IS NOT NULL
      THEN 'VENDOR'
    END                                                         AS lab_test_ordg_prov_vdr_id_qual,
    lab.practice_id                                             AS lab_test_exectg_fclty_vdr_id,
    CASE WHEN lab.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS lab_test_exectg_fclty_vdr_id_qual,
    COALESCE(lab.test_name, lab.test_name_specific)             AS lab_test_nm,
    lab.test_value_txt                                          AS lab_test_loinc_cd,
    lab.test_value_string                                       AS lab_result_nm,
    lab.test_value                                              AS lab_result_msrmt,
    lab.unit_of_measure                                         AS lab_result_uom,
    CASE WHEN lab.unit_of_measure IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS lab_result_qual,
    CASE
      WHEN lab.abnormal_flag_cd IS NULL THEN NULL
      WHEN SUBSTRING(UPPER(lab.abnormal_flag_cd), 1, 1) = 'N'
      THEN 'N' ELSE 'Y'
    END                                                         AS lab_result_abnorm_flg,
    CASE
      WHEN lab.min_norm = 'NULL'
      THEN NULL
      ELSE lab.min_norm
    END	AS lab_result_norm_min_msrmt,
    CASE
      WHEN lab.max_norm = 'NULL'
      THEN NULL
      ELSE lab.max_norm
    END                                                         AS lab_result_norm_max_msrmt,
    UPPER(lab.system_id)                                        AS data_src_cd,
    'lab'                                                       AS prmy_src_tbl_nm
FROM transactions_lab lab
    LEFT JOIN transactions_demographics dem ON lab.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
WHERE pay.state IS NULL OR pay.state <> 'state'
