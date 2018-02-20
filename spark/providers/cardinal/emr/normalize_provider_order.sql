SELECT
    CONCAT('40_', disp.id)                                      AS hv_prov_ord_id,
    disp.id                                                     AS vdr_prov_ord_id,
    CASE
      WHEN disp.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_prov_ord_id_qual,
    dem.patient_id                                              AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(disp.encounter_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                         AS ptnt_age_num,
    COALESCE(pay.gender, dem.gender)                            AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                              AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))  AS ptnt_zip3_cd,
    CONCAT('40_', disp.visit_id)                                AS hv_enc_id,
    EXTRACT_DATE(
        SUBSTRING(disp.encounter_date, 0, 8),
        '%Y%m%d'
        )                                                       AS enc_dt,
    EXTRACT_DATE(
        SUBSTRING(disp.admin_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS prov_ord_dt,
    disp.npi                                                    AS ordg_prov_npi,

-- only populate first name if str contains a comma, in which case the
-- first name is everything after the last comma
    CASE
      WHEN INSTR(disp.ordered_by_name, ',') > 0
      THEN TRIM(REVERSE(
              SPLIT(REVERSE(disp.ordered_by_name), ',')[0]
              ))
    END                                                         AS ordg_prov_frst_nm,

-- everything up to the last comma goes into the last name column
    CASE
      WHEN INSTR(disp.ordered_by_name, ',') > 0
      THEN TRIM(REGEXP_EXTRACT(disp.ordered_by_name, '(.*),', 1))
      ELSE disp.ordered_by_name
    END                                                         AS ordg_prov_last_nm,
    disp.practice_id                                            AS ordg_fclty_vdr_id,
    CASE
      WHEN disp.practice_id IS NOT NULL
      THEN 'VENDOR'
    END                                                         AS ordg_fclty_vdr_id_qual,
    EXTRACT_DATE(
        SUBSTRING(disp.scheduled_date, 0, 8),
        '%Y%m%d'
        )                                                       AS prov_ord_start_dt,
    EXTRACT_DATE(
        SUBSTRING(disp.discontinue_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS prov_ord_end_dt,
    UPPER(disp.type)                                            AS prov_ord_typ_cd,
    CASE WHEN disp.type IS NOT NULL THEN 'VENDOR' END           AS prov_ord_typ_cd_qual,
    UPPER(disp.cpt_code)                                        AS prov_ord_cd,
    COALESCE(disp.icd_ten, disp.icd_nine)                       AS prov_ord_diag_cd,
    CASE
      WHEN disp.icd_ten IS NOT NULL
      THEN '02'
      WHEN disp.icd_nine IS NOT NULL
      THEN '01'
    END                                                         AS prov_ord_diag_cd_qual,
    UPPER(disp.status)                                          AS prov_ord_stat_cd,
    CASE WHEN disp.status IS NOT NULL THEN 'VENDOR' END         AS prov_ord_stat_cd_qual,
    EXTRACT_DATE(
        SUBSTRING(disp.completed_date, 0, 8),
        '%Y%m%d'
        )                                                       AS prov_ord_complt_dt,
    EXTRACT_DATE(
        SUBSTRING(disp.cancelled_date, 0, 8),
        '%Y%m%d'
        )                                                       AS prov_ord_cxld_dt,
    CASE
      WHEN SUBSTRING(UPPER(COALESCE(disp.cancelled_ind, 'E')), 1, 1) IN ('0', 'N')
      THEN 'N'
      WHEN SUBSTRING(UPPER(COALESCE(disp.cancelled_ind, 'E')), 1, 1) IN ('1', 'Y')
      THEN 'Y'
    END                                                         AS prov_ord_cxld_flg,
    UPPER(disp.system_id)                                       AS data_src_cd,
    CASE
    WHEN INSTR(disp.create_date, '-') > 0
    THEN EXTRACT_DATE(
        SUBSTRING(disp.create_date, 0, 10),
        '%Y-%m-%d'
        )
    ELSE EXTRACT_DATE(
        SUBSTRING(disp.create_date, 0, 8),
        '%Y%m%d'
        )
    END                                                         AS data_captr_dt,
    'order_dispense'                                            AS prmy_src_tbl_nm
FROM transactions_dispense disp
    LEFT JOIN transactions_demographics dem ON disp.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
WHERE (pay.state IS NULL OR pay.state <> 'state') AND (disp.type IS NULL OR UPPER(disp.type) NOT IN ('MED', 'PROC'))
