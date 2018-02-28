SELECT
    CONCAT('40_enc_', enc.patient_visit_id)                     AS hv_proc_id,
    enc.patient_visit_id                                        AS vdr_proc_id,
    CASE
      WHEN enc.patient_visit_id IS NOT NULL THEN 'VISIT_ID'
    END                                                         AS vdr_proc_id_qual,
    enc.id                                                      AS vdr_alt_proc_id,
    CASE
      WHEN enc.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_proc_id_qual,
    enc.patient_id                                              AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(enc.visit_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                         AS ptnt_age_num,
    COALESCE(pay.gender, dem.gender)                            AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                              AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))  AS ptnt_zip3_cd,
    CONCAT('40_', enc.patient_visit_id)                         AS hv_enc_id,
    EXTRACT_DATE(
        SUBSTRING(enc.visit_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS enc_dt,
    EXTRACT_DATE(
        SUBSTRING(enc.visit_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS proc_dt,
    enc.practice_id                                             AS proc_rndrg_fclty_vdr_id,
    CASE WHEN enc.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS proc_rndrg_fclty_vdr_id_qual,
    enc.provider_npi                                            AS proc_rndrg_prov_npi,
    enc.cpt                                                     AS proc_cd,
    CASE WHEN enc.cpt IS NOT NULL THEN 'HC' END                 AS proc_cd_qual,
    UPPER(enc.system_id)                                        AS data_src_cd,
    'encounter'                                                 AS prmy_src_tbl_nm
FROM transactions_encounter enc
    LEFT JOIN transactions_demographics dem ON enc.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvjoinkey = pay.hvjoinkey
WHERE (pay.state IS NULL OR pay.state <> 'state') AND enc.cpt IS NOT NULL
