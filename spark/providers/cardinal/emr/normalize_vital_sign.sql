SELECT
    CONCAT('40_', enc.id)                                       AS hv_vit_sign_id,
    enc.id                                                      AS vdr_vit_sign_id,
    CASE
      WHEN enc.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_vit_sign_id_qual,
    enc.id                                                      AS vdr_alt_vit_sign_id,
    CASE
      WHEN enc.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_vit_sign_id_qual,
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
    enc.patient_visit_id                                        AS hv_enc_id,
    EXTRACT_DATE(
        SUBSTRING(enc.visit_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS enc_dt,
    EXTRACT_DATE(
        SUBSTRING(enc.visit_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS vit_sign_dt,
    enc.practice_id                                             AS vit_sign_rndrg_fclty_vdr_id,
    CASE WHEN enc.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS vit_sign_rndrg_fclty_vdr_id_qual,
    enc.provider_npi                                            AS vit_sign_rndrg_prov_npi,
    CASE n.n
    WHEN 0 THEN 'RAPID3'
    WHEN 1 THEN 'CDAI'
    WHEN 2 THEN 'SDAI'
    WHEN 3 THEN 'DAS28'
    WHEN 4 THEN 'STANFORD_HAQ'
    WHEN 5 THEN 'NJC28'
    WHEN 6 THEN 'TJC28'
    WHEN 7 THEN 'SJC28'
    WHEN 8 THEN 'PAIN'
    WHEN 9 THEN 'CARD_FN_RAW'
    WHEN 10 THEN 'CARD_FN_DEC'
    WHEN 11 THEN 'PGA'
    WHEN 12 THEN 'PTGA'
    END                                                         AS vit_sign_typ_cd,
    ARRAY(
        enc.rapid_3, enc.cdai, enc.sdai, enc.das28, enc.haq_score,
        enc.total_normal_28joint, enc.total_tender_28joint,
        enc.total_swollen_28joint, enc.pain_scale, enc.fn_raw,
        enc.fn_dec, enc.mdglobal_scale, enc.ptgl_dec
        )[n.n]                                                  AS vit_sign_msrmt,
    UPPER(enc.system_id)                                        AS data_src_cd,
    'encounter'                                                 AS prmy_src_tbl_nm
FROM transactions_encounter enc
    LEFT JOIN transactions_demographics dem ON enc.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvjoinkey = pay.hvjoinkey
    CROSS JOIN vit_sign_exploder n
WHERE (pay.state IS NULL OR pay.state <> 'state')
    AND ARRAY(
        enc.rapid_3, enc.cdai, enc.sdai, enc.das28, enc.haq_score,
        enc.total_normal_28joint, enc.total_tender_28joint,
        enc.total_swollen_28joint, enc.pain_scale, enc.fn_raw,
        enc.fn_dec, enc.mdglobal_scale, enc.ptgl_dec
        )[n.n] IS NOT NULL
