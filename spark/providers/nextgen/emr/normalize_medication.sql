INSERT INTO medication_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_medctn_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    med.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    med.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_medctn_ord_id
    NULL,                                   -- vdr_medctn_ord_id_qual
    NULL,                                   -- vdr_medctn_admin_id
    NULL,                                   -- vdr_medctn_admin_id_qual
    concat_ws('_', 'NG',
        med.reportingenterpriseid,
        med.nextgengroupid) as hvid,        -- hvid
    dem.birthyear,                          -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END,                       -- ptnt_gender_cd
    NULL,                                   -- ptnt_state_cd
    dem.zip3,                               -- ptnt_zip3_cd
    concat_ws('_', '35',
        med.reportingenterpriseid,
        med.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(med.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(med.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- medctn_ord_dt
    extract_date(
        substring(med.startdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- medctn_admin_dt
    NULL,                                   -- medctn_rndrg_fclty_npi
    NULL,                                   -- medctn_rndrg_fclty_vdr_id
    NULL,                                   -- medctn_rndrg_fclty_vdr_id_qual
    NULL,                                   -- medctn_rndrg_fclty_alt_id
    NULL,                                   -- medctn_rndrg_fclty_alt_id_qual
    NULL,                                   -- medctn_rndrg_fclty_tax_id
    NULL,                                   -- medctn_rndrg_fclty_dea_id
    NULL,                                   -- medctn_rndrg_fclty_state_lic_id
    NULL,                                   -- medctn_rndrg_fclty_comrcl_id
    NULL,                                   -- medctn_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- medctn_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- medctn_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- medctn_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- medctn_rndrg_fclty_alt_speclty_id
    NULL,                                   -- medctn_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- medctn_rndrg_fclty_nm
    NULL,                                   -- medctn_rndrg_fclty_addr_1_txt
    NULL,                                   -- medctn_rndrg_fclty_addr_2_txt
    NULL,                                   -- medctn_rndrg_fclty_state_cd
    NULL,                                   -- medctn_rndrg_fclty_zip_cd
    NULL,                                   -- medctn_ordg_prov_npi
    NULL,                                   -- medctn_ordg_prov_vdr_id
    NULL,                                   -- medctn_ordg_prov_vdr_id_qual
    NULL,                                   -- medctn_ordg_prov_alt_id
    NULL,                                   -- medctn_ordg_prov_alt_id_qual
    NULL,                                   -- medctn_ordg_prov_tax_id
    NULL,                                   -- medctn_ordg_prov_dea_id
    NULL,                                   -- medctn_ordg_prov_state_lic_id
    NULL,                                   -- medctn_ordg_prov_comrcl_id
    NULL,                                   -- medctn_ordg_prov_upin
    NULL,                                   -- medctn_ordg_prov_ssn
    NULL,                                   -- medctn_ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- medctn_ordg_prov_alt_taxnmy_id
    NULL,                                   -- medctn_ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- medctn_ordg_prov_mdcr_speclty_cd
    NULL,                                   -- medctn_ordg_prov_alt_speclty_id
    NULL,                                   -- medctn_ordg_prov_alt_speclty_id_qual
    NULL,                                   -- medctn_ordg_prov_frst_nm
    NULL,                                   -- medctn_ordg_prov_last_nm
    NULL,                                   -- medctn_ordg_prov_addr_1_txt
    NULL,                                   -- medctn_ordg_prov_addr_2_txt
    NULL,                                   -- medctn_ordg_prov_state_cd
    NULL,                                   -- medctn_ordg_prov_zip_cd
    NULL,                                   -- medctn_adminrg_fclty_npi
    NULL,                                   -- medctn_adminrg_fclty_vdr_id
    NULL,                                   -- medctn_adminrg_fclty_vdr_id_qual
    NULL,                                   -- medctn_adminrg_fclty_alt_id
    NULL,                                   -- medctn_adminrg_fclty_alt_id_qual
    NULL,                                   -- medctn_adminrg_fclty_tax_id
    NULL,                                   -- medctn_adminrg_fclty_dea_id
    NULL,                                   -- medctn_adminrg_fclty_state_lic_id
    NULL,                                   -- medctn_adminrg_fclty_comrcl_id
    NULL,                                   -- medctn_adminrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- medctn_adminrg_fclty_alt_taxnmy_id
    NULL,                                   -- medctn_adminrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- medctn_adminrg_fclty_mdcr_speclty_cd
    NULL,                                   -- medctn_adminrg_fclty_alt_speclty_id
    NULL,                                   -- medctn_adminrg_fclty_alt_speclty_id_qual
    NULL,                                   -- medctn_adminrg_fclty_nm
    NULL,                                   -- medctn_adminrg_fclty_addr_1_txt
    NULL,                                   -- medctn_adminrg_fclty_addr_2_txt
    NULL,                                   -- medctn_adminrg_fclty_state_cd
    NULL,                                   -- medctn_adminrg_fclty_zip_cd
    NULL,                                   -- rx_num
    extract_date(
        substring(med.startdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- medctn_start_dt
    extract_date(
        substring(med.datestopped, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- medctn_end_dt
    CASE WHEN icd_diag_codes.code IS NOT NULL THEN icd_diag_codes.code
        ELSE NULL END,                      -- medctn_diag_cd
    NULL,                                   -- medctn_diag_cd_qual
    med.emrcode,                            -- medctn_ndc
    NULL,                                   -- medctn_lblr_cd
    NULL,                                   -- medctn_drug_and_strth_cd
    NULL,                                   -- medctn_pkg_cd
    med.hiclsqno,                           -- medctn_hicl_thrptc_cls_cd
    med.hic3,                               -- medctn_hicl_cd
    med.gcnseqno,                           -- medctn_gcn_cd
    med.rxnorm,                             -- medctn_rxnorm_cd
    NULL,                                   -- medctn_snomed_cd
    NULL,                                   -- medctn_genc_ok_flg
    NULL,                                   -- medctn_brd_nm
    NULL,                                   -- medctn_genc_nm
    CASE WHEN med.med_class_id = 'O' THEN 'N'
        WHEN med.med_class_id = 'F' THEN 'Y'
        ELSE NULL END,                      -- medctn_rx_flg
    med.rxquantity,                         -- medctn_rx_qty
    NULL,                                   -- medctn_dly_qty
    NULL,                                   -- medctn_dispd_qty
    NULL,                                   -- medctn_days_supply_qty
    NULL,                                   -- medctn_admin_unt_qty
    NULL,                                   -- medctn_admin_freq_qty
    NULL,                                   -- medctn_admin_sched_cd
    NULL,                                   -- medctn_admin_sched_qty
    clean_up_freetext(med.sigcodes, false), -- medctn_admin_sig_cd
    clean_up_freetext(med.sigdesc, false),  -- medctn_admin_sig_txt
    NULL,                                   -- medctn_admin_form_nm
    NULL,                                   -- medctn_specl_pkgg_cd
    NULL,                                   -- medctn_strth_txt
    NULL,                                   -- medctn_strth_txt_qual
    med.dose,                               -- medctn_dose_txt
    NULL,                                   -- medctn_dose_txt_qual
    NULL,                                   -- medctn_admin_rte_txt
    med.orgrefills,                         -- medctn_orig_rfll_qty
    NULL,                                   -- medctn_fll_num
    med.rxrefills,                          -- medctn_remng_rfll_qty
    extract_date(
        substring(med.datelastrefilled, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- medctn_last_rfll_dt
    NULL,                                   -- medctn_smpl_flg
    NULL,                                   -- medctn_elect_rx_flg
    NULL,                                   -- medctn_verfd_flg
    NULL,                                   -- medctn_prod_svc_id
    NULL,                                   -- medctn_prod_svc_id_qual
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'medicationorder',                      -- prmy_src_tbl_nm
    extract_date(
        substring(med.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM medicationorder med
    LEFT JOIN demographics_dedup dem ON med.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND med.NextGenGroupID = dem.NextGenGroupID
    CROSS JOIN medication_exploder n
    LEFT JOIN icd_diag_codes ON clean_up_freetext(trim(split(med.diagnosis_code_id, ',')[n.n]), true) = icd_diag_codes.code
WHERE (split(med.diagnosis_code_id, ',')[n.n] IS NOT NULL
        AND trim(split(med.diagnosis_code_id, ',')[n.n]) != '')
    OR (n.n = 0 AND trim(clean_up_freetext(med.diagnosis_code_id, true)) IS NULL)
DISTRIBUTE BY hvid;

ALTER TABLE medication_common_model RENAME TO medication_common_model_bak;
CREATE TABLE medication_common_model AS SELECT DISTINCT * FROM medication_common_model_bak;
DROP TABLE medication_common_model_bak;
