SELECT
    CONCAT('40_', enc.patient_visit_id)                         AS hv_enc_id,
    enc.patient_visit_id                                        AS vdr_enc_id,
    CASE
      WHEN enc.patient_visit_id IS NOT NULL THEN 'VISIT_ID'
    END                                                         AS vdr_enc_id_qual,
    enc.id                                                      AS vdr_alt_enc_id,
    CASE
      WHEN enc.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_enc_id_qual,
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
    EXTRACT_DATE(
        SUBSTRING(enc.visit_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS enc_start_dt,
    EXTRACT_DATE(
        SUBSTRING(enc.visit_end_tstamp, 0, 10),
        '%Y-%m-%d'
        )                                                       AS enc_end_dt,
    enc.practice_id                                             AS enc_rndrg_fclty_vdr_id,
    CASE WHEN enc.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS enc_rndrg_fclty_vdr_id_qual,
    enc.provider_npi                                            AS enc_rndrg_prov_npi,
    UPPER(enc.visit_type)                                       AS enc_typ_cd,
    CASE WHEN enc.visit_type IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS enc_typ_cd_qual,
    enc.inst_id                                                 AS enc_pos_cd,
    UPPER(enc.system_id)                                        AS data_src_cd,
    'encounter'                                                 AS prmy_src_tbl_nm
FROM transactions_encounter enc
    LEFT JOIN transactions_demographics dem ON enc.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvjoinkey = pay.hvjoinkey
WHERE pay.state IS NULL OR pay.state <> 'state'
