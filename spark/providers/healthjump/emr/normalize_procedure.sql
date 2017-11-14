INSERT INTO procedure_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_proc_id
    NULL,                                   -- crt_dt
    '4',                                    -- mdl_vrsn_num
    NULL,                                   -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    NULL,                                   -- vdr_org_id
    NULL,                                   -- vdr_diag_id
    NULL,                                   -- vdr_diag_id_qual
    pay.hvid,                               -- hvid
    pay.yearOfBirth,                        -- ptnt_birth_yr
    NULL,                                   -- ptnt_age_num
    NULL,                                   -- ptnt_lvg_flg
    NULL,                                   -- ptnt_dth_dt
    CASE
      WHEN UPPER(COALESCE(pay.gender, dem.gender)) IN ('F', 'M')
      THEN UPPER(COALESCE(pay.gender, dem.gender))
      ELSE 'U'
    END,                                    -- ptnt_gender_cd
    UPPER(COALESCE(pay.state, dem.state)),  -- ptnt_state_cd
    pay.threeDigitZip,                      -- ptnt_zip3_cd
    NULL,                                   -- hv_enc_id
    NULL,                                   -- enc_dt
    NULL,                                   -- enc_dt
    CASE
      WHEN LENGTH(proc.date) = 8 AND LOCATE('/', proc.date) = 0
      THEN EXTRACT_DATE(proc.date, '%Y%m%d')
      WHEN LENGTH(proc.date) = 10 AND LOCATE('/', proc.date) <> 0
      THEN EXTRACT_DATE(proc.date, '%m/%d/%Y')
    END,                                    -- proc_dt
    NULL,                                   -- proc_rndrg_fclty_npi
    NULL,                                   -- proc_rndrg_fclty_vdr_id
    NULL,                                   -- proc_rndrg_fclty_vdr_id_qual
    NULL,                                   -- proc_rndrg_fclty_alt_id
    NULL,                                   -- proc_rndrg_fclty_alt_id_qual
    NULL,                                   -- proc_rndrg_fclty_tax_id
    NULL,                                   -- proc_rndrg_fclty_dea_id
    NULL,                                   -- proc_rndrg_fclty_state_lic_id
    NULL,                                   -- proc_rndrg_fclty_comrcl_id
    NULL,                                   -- proc_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- proc_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- proc_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- proc_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- proc_rndrg_fclty_alt_speclty_id
    NULL,                                   -- proc_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- proc_rndrg_fclty_nm
    NULL,                                   -- proc_rndrg_fclty_addr_1_txt
    NULL,                                   -- proc_rndrg_fclty_addr_2_txt
    NULL,                                   -- proc_rndrg_fclty_state_cd
    NULL,                                   -- proc_rndrg_fclty_zip_cd
    NULL,                                   -- proc_rndrg_prov_npi
    NULL,                                   -- proc_rndrg_prov_vdr_id
    NULL,                                   -- proc_rndrg_prov_vdr_id_qual
    NULL,                                   -- proc_rndrg_prov_alt_id
    NULL,                                   -- proc_rndrg_prov_alt_id_qual
    NULL,                                   -- proc_rndrg_prov_tax_id
    NULL,                                   -- proc_rndrg_prov_dea_id
    NULL,                                   -- proc_rndrg_prov_state_lic_id
    NULL,                                   -- proc_rndrg_prov_comrcl_id
    NULL,                                   -- proc_rndrg_prov_upin
    NULL,                                   -- proc_rndrg_prov_ssn
    NULL,                                   -- proc_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- proc_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- proc_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- proc_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- proc_rndrg_prov_alt_speclty_id
    NULL,                                   -- proc_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- proc_rndrg_prov_frst_nm
    NULL,                                   -- proc_rndrg_prov_last_nm
    NULL,                                   -- proc_rndrg_prov_addr_1_txt
    NULL,                                   -- proc_rndrg_prov_addr_2_txt
    NULL,                                   -- proc_rndrg_prov_state_cd
    NULL,                                   -- proc_rndrg_prov_zip_cd
    CASE
      WHEN LENGTH(CLEAN_UP_PROCEDURE_CODE(proc.cpt)) >= 5
      THEN SUBSTRING(CLEAN_UP_PROCEDURE_CODE(proc.cpt), 0, 5)
    END,                                    -- proc_cd
    NULL,                                   -- proc_cd_qual
    CASE
      WHEN LENGTH(CLEAN_UP_PROCEDURE_CODE(proc.cpt)) = 7
      THEN SUBSTRING(CLEAN_UP_PROCEDURE_CODE(proc.cpt), 5, 2)
      WHEN LENGTH(CLEAN_UP_PROCEDURE_CODE(proc.cpt)) = 2
      THEN CLEAN_UP_PROCEDURE_CODE(proc.cpt)
    END,                                    -- proc_cd_1_modfr
    NULL,                                   -- proc_cd_2_modfr
    NULL,                                   -- proc_cd_3_modfr
    NULL,                                   -- proc_cd_4_modfr
    NULL,                                   -- proc_cd_modfr_qual
    NULL,                                   -- proc_snomed_cd
    NULL,                                   -- proc_prty_cd
    NULL,                                   -- proc_prty_cd_qual
    NULL,                                   -- proc_alt_cd
    NULL,                                   -- proc_alt_cd_qual
    NULL,                                   -- proc_pos_cd
    NULL,                                   -- proc_unit_qty
    NULL,                                   -- proc_uom
    NULL,                                   -- proc_diag_cd
    NULL,                                   -- proc_diag_cd_qual
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    NULL                                    -- prmy_src_tbl_nm
FROM transactions_procedure proc
    LEFT JOIN transactions_demographics dem ON proc.id = dem.demographicid
    LEFT JOIN matching_payload pay ON dem.hvJoinKey = pay.hvJoinKey
    ;
