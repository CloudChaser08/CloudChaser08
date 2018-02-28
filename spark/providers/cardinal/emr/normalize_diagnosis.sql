SELECT
    CONCAT('40_', diag.diagnosis_id)                            AS hv_diag_id,
    diag.diagnosis_id                                           AS vdr_diag_id,
    CASE
      WHEN diag.diagnosis_id IS NOT NULL THEN 'DIAGNOSIS_ID'
    END                                                         AS vdr_diag_id_qual,
    diag.id                                                     AS vdr_alt_diag_id,
    CASE
      WHEN diag.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_diag_id_qual,
    diag.patient_id                                             AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(diag.diagnosis_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                         AS ptnt_age_num,
    COALESCE(pay.gender, dem.gender)                            AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                              AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))  AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTRING(diag.diagnosis_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS diag_dt,
    diag.practice_id                                            AS diag_rndrg_fclty_vdr_id,
    CASE WHEN diag.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS diag_rndrg_fclty_vdr_id_qual,
    diag.provider_npi                                           AS diag_rndrg_prov_npi,
    EXTRACT_DATE(
        SUBSTRING(diag.resolution_date, 0, 8),
        '%Y%m%d'
        )                                                       AS diag_resltn_dt,
    diag.icd_cd                                                 AS diag_cd,
    CASE
    WHEN diag.diagnosis_typ REGEXP '^[0-9]+$' THEN CAST(diag.diagnosis_typ AS INT)
    WHEN UPPER(diag.diagnosis_typ) = 'PRIMARY' THEN 1
    WHEN UPPER(diag.diagnosis_typ) = 'SECONDARY' THEN 2
    END                                                         AS diag_prty_cd,
    diag.resolution_desc                                        AS diag_resltn_desc,
    UPPER(diag.mthd_of_diagnosis)                               AS diag_meth_cd,
    CASE WHEN diag.mthd_of_diagnosis IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS diag_meth_cd_qual,
    UPPER(diag.system_id)                                       AS data_src_cd,
    'diagnosis'                                                 AS prmy_src_tbl_nm
FROM transactions_diagnosis diag
    LEFT JOIN transactions_demographics dem ON diag.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvjoinkey = pay.hvjoinkey
WHERE pay.state IS NULL OR pay.state <> 'state'
