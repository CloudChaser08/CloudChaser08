SELECT
    CONCAT('40_ord_', disp.id)                                  AS hv_proc_id,
    disp.id                                                     AS vdr_proc_id,
    CASE
      WHEN disp.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_proc_id_qual,
    disp.id                                                     AS vdr_alt_proc_id,
    CASE
      WHEN disp.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_alt_proc_id_qual,
    disp.patient_id                                             AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(disp.encounter_date, 1, 4), 1)
    AND COALESCE(pay.age, dem.patient_age) = 0
    THEN NULL
    ELSE COALESCE(pay.age, dem.patient_age)
    END                                                         AS ptnt_age_num,
    CASE
    WHEN UPPER(COALESCE(pay.gender, dem.gender)) IN ('M', 'F', 'U')
    THEN UPPER(COALESCE(pay.gender, dem.gender))
    ELSE 'U'
    END                                                         AS ptnt_gender_cd,
    COALESCE(pay.state, dem.state)                              AS ptnt_state_cd,
    COALESCE(pay.threeDigitZip, SUBSTRING(dem.zip_code, 0, 3))  AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTRING(disp.encounter_date, 0, 8),
        '%Y%m%d'
        )                                                       AS enc_dt,
    EXTRACT_DATE(
        SUBSTRING(disp.admin_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS proc_dt,
    disp.practice_id                                            AS proc_rndrg_fclty_vdr_id,
    CASE WHEN disp.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS proc_rndrg_fclty_vdr_id_qual,
    disp.npi                                                    AS proc_rndrg_prov_npi,
    disp.cpt_code                                               AS proc_cd,
    CASE WHEN disp.cpt_code IS NOT NULL
    THEN 'HC'
    END                                                         AS proc_cd_qual,
    disp.qty                                                    AS proc_unit_qty,
    COALESCE(disp.icd_ten, disp.icd_nine)                       AS proc_diag_cd,
    CASE
    WHEN disp.icd_ten IS NOT NULL
    THEN '02'
    WHEN disp.icd_nine IS NOT NULL
    THEN '01'
    END                                                         AS proc_diag_cd_qual,
    disp.system_id                                              AS data_src_cd,
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
WHERE (pay.state IS NULL OR pay.state <> 'state') AND UPPER(disp.type) = 'PROC'
