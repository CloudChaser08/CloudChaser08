SELECT
    CASE WHEN LENGTH(TRIM(UPPER(lab.id))) BETWEEN 30 AND 45 AND lab.id NOT LIKE '% %'
    THEN CONCAT('40_', lab.id)
    END                                                             AS hv_vit_sign_id,
    CASE WHEN LENGTH(TRIM(UPPER(lab.id))) BETWEEN 30 AND 45 AND lab.id NOT LIKE '% %'
    THEN lab.id
    END                                                             AS vdr_vit_sign_id,
    CASE WHEN lab.id IS NOT NULL AND LENGTH(TRIM(UPPER(lab.id))) BETWEEN 30 AND 45 AND lab.id NOT LIKE '% %'
    THEN 'VENDOR_ROW_ID'
    END                                                             AS vdr_vit_sign_id_qual,
    lab.patient_id                                                  AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))      AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(lab.test_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                             AS ptnt_age_num,
    CASE
    WHEN UPPER(COALESCE(pay.gender, dem.gender)) IN ('M', 'F', 'U')
    THEN UPPER(COALESCE(pay.gender, dem.gender))
    ELSE 'U'
    END                                                             AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                                  AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))      AS ptnt_zip3_cd,
    lab.test_date                                                   AS vit_sign_dt,
    lab.practice_id                                                 AS vit_sign_rndrg_fclty_vdr_id,
    CASE WHEN lab.practice_id IS NOT NULL THEN 'VENDOR' END         AS vit_sign_rndrg_fclty_vdr_id_qual,
    lab.physician_id                                                AS vit_sign_rndrg_prov_vdr_id,
    CASE WHEN lab.physician_id IS NOT NULL THEN 'PHYSICIAN_ID' END  AS vit_sign_rndrg_prov_vdr_id_qual,
    CASE
    WHEN TRIM(UPPER(lab.test_name)) = 'HEIGHT_CM' THEN 'HEIGHT'
    WHEN TRIM(UPPER(lab.test_name)) = 'WEIGHT_KG' THEN 'WEIGHT'
    END                                                             AS vit_sign_typ_cd,
    'TEST_NAME'                                                     AS vit_sign_typ_cd_qual,
    CASE
    WHEN TRIM(UPPER(lab.test_name)) = 'HEIGHT_CM'
    THEN CONVERT_VALUE(lab.test_value, 'CENTIMETERS_TO_INCHES')
    WHEN TRIM(UPPER(lab.test_name)) = 'WEIGHT_KG'
    THEN CONVERT_VALUE(lab.test_value, 'KILOGRAMS_TO_POUNDS')
    END                                                             AS vit_sign_msrmt,
    CASE
    WHEN TRIM(UPPER(lab.test_name)) = 'HEIGHT_CM' THEN 'INCHES'
    WHEN TRIM(UPPER(lab.test_name)) = 'WEIGHT_KG' THEN 'POUNDS'
    END                                                             AS vit_sign_uom,
    CASE
    WHEN 0 = LENGTH(TRIM(COALESCE(lab.abnormal_flag_cd, ''))) THEN NULL
    WHEN SUBSTR(TRIM(UPPER(COALESCE(lab.abnormal_flag_cd, ''))), 1, 1) = 'N' THEN 'N'
    WHEN SUBSTR(TRIM(UPPER(COALESCE(lab.abnormal_flag_cd, ''))), 1, 1) = 'Y' THEN 'Y'
    END                                                             AS vit_sign_abnorm_flg,
    UPPER(lab.system_id)                                            AS data_src_cd,
    'lab'                                                           AS prmy_src_tbl_nm
FROM transactions_lab lab
    LEFT OUTER JOIN transactions_demographics dem ON lab.patient_id = dem.patient_id
    LEFT OUTER JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
WHERE TRIM(UPPER(lab.test_name)) IN ('HEIGHT_CM', 'WEIGHT_KG')
