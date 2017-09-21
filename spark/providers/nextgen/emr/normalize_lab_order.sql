INSERT INTO lab_order_common_model
SELECT
    NULL,                                   -- row_id
    concat_ws('_', '35', ord.reportingenterpriseid,
        ord.OrderNum),                      -- hv_lab_ord_id
    NULL,                                   -- crt_dt
    '03',                                   -- mdl_vrsn_num
    ord.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    ord.reportingenterpriseid,              -- vdr_org_id
    ord.ordernum,                           -- vdr_lab_ord_id
    'reportingenterpriseid',                 -- vdr_lab_ord_id_qual
    concat_ws('_', 'NG',
        ord.reportingenterpriseid,
        ord.nextgengroupid) as hvid,        -- hvid
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
        ord.reportingenterpriseid,
        ord.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    NULL,                                   -- prov_ord_dt
    extract_date(
        substring(ord.scheduledtime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- lab_ord_test_schedd_dt
    extract_date(
        substring(ord.collectiontime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- lab_ord_smpl_collctn_dt
    NULL,                                   -- lab_ord_ordg_prov_npi
    NULL,                                   -- lab_ord_ordg_prov_vdr_id
    NULL,                                   -- lab_ord_ordg_prov_vdr_id_qual
    NULL,                                   -- lab_ord_ordg_prov_alt_id
    NULL,                                   -- lab_ord_ordg_prov_alt_id_qual
    NULL,                                   -- lab_ord_ordg_prov_tax_id
    NULL,                                   -- lab_ord_ordg_prov_dea_id
    NULL,                                   -- lab_ord_ordg_prov_state_lic_id
    NULL,                                   -- lab_ord_ordg_prov_comrcl_id
    NULL,                                   -- lab_ord_ordg_prov_upin
    NULL,                                   -- lab_ord_ordg_prov_ssn
    NULL,                                   -- lab_ord_ordg_prov_nucc_taxnmy_cd
    NULL,                                   -- lab_ord_ordg_prov_alt_taxnmy_id
    NULL,                                   -- lab_ord_ordg_prov_alt_taxnmy_id_qual
    NULL,                                   -- lab_ord_ordg_prov_mdcr_speclty_cd
    NULL,                                   -- lab_ord_ordg_prov_alt_speclty_id
    NULL,                                   -- lab_ord_ordg_prov_alt_speclty_id_qual
    NULL,                                   -- lab_ord_ordg_prov_fclty_nm
    NULL,                                   -- lab_ord_ordg_prov_frst_nm
    NULL,                                   -- lab_ord_ordg_prov_last_nm
    NULL,                                   -- lab_ord_ordg_prov_addr_1_txt
    NULL,                                   -- lab_ord_ordg_prov_addr_2_txt
    NULL,                                   -- lab_ord_ordg_prov_state_cd
    NULL,                                   -- lab_ord_ordg_prov_zip_cd
    ord.loinccode,                          -- lab_ord_loinc_cd
    clean_up_freetext(ord.snomedcode, false),
                                            -- lab_ord_snomed_cd
    clean_up_freetext(ord.testcodeid, false),
                                            -- lab_ord_alt_cd
    NULL,                                   -- lab_ord_alt_cd_qual
    clean_up_freetext(ord.emrcode, false),  -- lab_ord_test_nm
    NULL,                                   -- lab_ord_panel_nm
    trim(split(ord.diagnoses, ',')[n.n]),   -- lab_ord_diag_cd
    NULL,                                   -- lab_ord_diag_cd_qual
    extract_date(
        substring(ord.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- data_captr_dt
    clean_up_freetext(ord.ngnstatus, false),
                                            -- rec_stat_cd
    'laborder',                             -- prmy_src_tbl_nm
    extract_date(
        substring(ord.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM laborder ord
    LEFT JOIN demographics_dedup dem ON ord.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND ord.NextGenGroupID = dem.NextGenGroupID
    CROSS JOIN lab_order_exploder n
WHERE (split(ord.diagnoses, ',')[n.n] IS NOT NULL AND trim(split(ord.diagnoses, ',')[n.n]) != '')
    OR (n.n = 0 AND regexp_extract(ord.diagnoses, '([^,\\s])') IS NULL)
DISTRIBUTE BY hvid;
