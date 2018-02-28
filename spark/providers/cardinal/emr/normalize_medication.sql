SELECT
    CONCAT('40_', med.id)                                       AS hv_medctn_id,
    med.id                                                      AS vdr_medctn_ord_id,
    CASE
    WHEN med.id IS NOT NULL THEN 'VENDOR_ROW_ID'
    END                                                         AS vdr_medctn_ord_id_qual,
    med.patient_id                                              AS hvid,
    COALESCE(pay.yearOfBirth, SUBSTRING(dem.birth_date, 0, 4))  AS ptnt_birth_yr,
    CASE
    WHEN COALESCE(
        pay.yearOfBirth, SUBSTRING(dem.birth_date, 1, 4), 0
        ) <> COALESCE(SUBSTRING(med.encounter_date, 1, 4), 1)
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
    CONCAT('40_', med.visit_id)                                 AS hv_enc_id,
    EXTRACT_DATE(
        SUBSTRING(med.encounter_date, 0, 8),
        '%Y%m%d'
        )                                                       AS enc_dt,
    EXTRACT_DATE(
        SUBSTRING(med.scheduled_date, 0, 8),
        '%Y%m%d'
        )                                                       AS medctn_ord_dt,
    EXTRACT_DATE(
        SUBSTRING(med.admin_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS medctn_admin_dt,
    med.practice_id                                             AS medctn_rndrg_fclty_vdr_id,
    CASE WHEN med.practice_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS medctn_rndrg_fclty_vdr_id_qual,
    med.npi                                                     AS medctn_ordg_prov_npi,

-- only populate first name if str contains a comma, in which case the
-- first name is everything after the last comma
    CASE
    WHEN INSTR(med.ordered_by_name, ',') > 0
    THEN TRIM(REVERSE(
            SPLIT(REVERSE(med.ordered_by_name), ',')[0]
            ))
    END                                                         AS medctn_ordg_prov_frst_nm,

-- everything up to the last comma goes into the last name column
    CASE
    WHEN INSTR(med.ordered_by_name, ',') > 0
    THEN TRIM(REGEXP_EXTRACT(med.ordered_by_name, '(.*),', 1))
    ELSE med.ordered_by_name
    END                                                         AS medctn_ordg_prov_last_nm,
    med.dest_pharm_id                                           AS medctn_adminrg_fclty_vdr_id,
    CASE WHEN med.dest_pharm_id IS NOT NULL
    THEN 'VENDOR'
    END                                                         AS medctn_adminrg_fclty_vdr_id_qual,
    med.prescription_number                                     AS rx_num,
    EXTRACT_DATE(
        SUBSTRING(med.discontinue_date, 0, 10),
        '%Y-%m-%d'
        )                                                       AS medctn_end_dt,
    med.duration                                                AS medctn_ord_durtn_day_cnt,
    UPPER(med.status)                                           AS medctn_ord_stat_cd,
    EXTRACT_DATE(
        SUBSTRING(med.cancelled_date, 0, 8),
        '%Y%m%d'
        )                                                       AS medctn_ord_cxld_dt,
    CASE
    WHEN SUBSTRING(UPPER(COALESCE(cancelled_ind, 'E')), 1, 1) IN ('0', 'N')
    THEN 'N'
    WHEN SUBSTRING(UPPER(COALESCE(cancelled_ind, 'E')), 1, 1) IN ('1', 'Y')
    THEN 'Y'
    END                                                         AS medctn_ord_cxld_flg,
    COALESCE(med.icd_ten, med.icd_nine)                         AS medctn_diag_cd,
    CASE
    WHEN med.icd_ten IS NOT NULL THEN '02'
    WHEN med.icd_nine IS NOT NULL THEN '01'
    END                                                         AS medctn_diag_cd_qual,
    med.ndc_written                                             AS medctn_ndc,
    CLEAN_UP_NUMERIC_CODE(med.ndc_labeler_code)                 AS medctn_lblr_cd,
    CLEAN_UP_NUMERIC_CODE(med.ndc_drug_and_strength)            AS medctn_drug_and_strth_cd,
    CLEAN_UP_NUMERIC_CODE(med.ndc_package_size)                 AS medctn_pkg_cd,
    med.brand_name                                              AS medctn_brd_nm,
    med.generic_name                                            AS medctn_genc_nm,
    CASE WHEN SUBSTRING(UPPER(COALESCE(prescribed_else_ind, 'E')), 1, 1) IN ('0', 'N')
    THEN 'N'
    WHEN SUBSTRING(UPPER(COALESCE(prescribed_else_ind, 'E')), 1, 1) IN ('1', 'Y')
    THEN 'Y'
    END                                                         AS medctn_extrnl_rx_flg,
    med.quantity_per_day                                        AS medctn_dly_qty,
    med.qty                                                     AS medctn_dispd_qty,
    med.days_supply                                             AS medctn_days_supply_qty,
    med.num_doses                                               AS medctn_admin_unt_qty,
    UPPER(med.dosage_form)                                      AS medctn_admin_form_nm,
    COALESCE(med.dose_strength, med.strength)                   AS medctn_strth_txt,
    med.strength_unit                                           AS medctn_strth_txt_qual,
    UPPER(med.order_dose)                                       AS medctn_dose_txt,
    CASE WHEN med.order_dose IS NOT NULL THEN 'VENDOR' END      AS medctn_dose_txt_qual,
    UPPER(med.uom_description)                                  AS medctn_dose_uom,
    COALESCE(med.route, med.admn_route)                         AS medctn_admin_rte_txt,
    med.refills                                                 AS medctn_remng_rfll_qty,
    UPPER(med.system_id)                                        AS data_src_cd,
    CASE
    WHEN INSTR(med.create_date, '-') > 0
    THEN EXTRACT_DATE(
        SUBSTRING(med.create_date, 0, 10),
        '%Y-%m-%d'
        )
    ELSE EXTRACT_DATE(
        SUBSTRING(med.create_date, 0, 8),
        '%Y%m%d'
        )
    END                                                         AS data_captr_dt,
    'order_dispense'                                            AS prmy_src_tbl_nm
FROM transactions_dispense med
    LEFT JOIN transactions_demographics dem ON med.patient_id = dem.patient_id
    LEFT JOIN matching_payload pay ON dem.hvjoinkey = pay.hvjoinkey
WHERE (pay.state IS NULL OR pay.state <> 'state') AND UPPER(med.type) = 'MED'
