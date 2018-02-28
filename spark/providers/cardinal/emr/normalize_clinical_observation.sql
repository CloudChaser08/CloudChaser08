SELECT
    CONCAT('40_', diag.diagnosis_id)                            AS hv_clin_obsn_id,
    diag.diagnosis_id                                           AS vdr_clin_obsn_id,
    CASE WHEN diag.diagnosis_id IS NOT NULL
    THEN 'DIAGNOSIS_ID'
    END                                                         AS vdr_clin_obsn_id_qual,
    diag.id                                                     AS vdr_alt_clin_obsn_id,
    CASE WHEN diag.id IS NOT NULL
    THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_clin_obsn_id_qual,
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
        )                                                       AS clin_obsn_dt,
    diag.practice_id                                            AS clin_obsn_rndrg_fclty_vdr_id,
    CASE WHEN diag.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS clin_obsn_rndrg_fclty_vdr_id_qual,
    diag.provider_npi                                           AS clin_obsn_rndrg_prov_npi,
    EXTRACT_DATE(
        SUBSTRING(diag.resolution_date, 0, 8),
        '%Y%m%d'
        )                                                       AS clin_obsn_resltn_dt,
    diag.clinical_desc                                          AS clin_obsn_desc,
    diag.icd_cd                                                 AS clin_obsn_diag_cd,
    ARRAY(
        diag.stg_crit_desc, diag.stage_of_disease, diag.cancer_stage,
        diag.cancer_stage_t, diag.cancer_stage_n, diag.cancer_stage_m
        )[n.n]                                                  AS clin_obsn_result_cd,
    CASE n.n
    WHEN 0 THEN 'CANCER_STAGE_PATHOLOGY'
    WHEN 1 THEN 'DISEASE_STAGE'
    WHEN 2 THEN 'CANCER_STAGE'
    WHEN 3 THEN 'CANCER_STAGE_T'
    WHEN 4 THEN 'CANCER_STAGE_N'
    WHEN 5 THEN 'CANCER_STAGE_M'
    END                                                         AS clin_obsn_result_cd_qual,
    UPPER(diag.system_id)                                       AS data_src_cd,
    'diagnosis'                                                 AS prmy_src_tbl_nm
FROM transactions_diagnosis diag
    LEFT JOIN transactions_demographics dem ON diag.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
    CROSS JOIN clin_obs_exploder n
WHERE ARRAY(
        diag.stg_crit_desc, diag.stage_of_disease, diag.cancer_stage,
        diag.cancer_stage_t, diag.cancer_stage_n, diag.cancer_stage_m
        )[n.n] IS NOT NULL AND (pay.state IS NULL OR pay.state <> 'state')
