SELECT
    NULL                  AS row_id,
    CONCAT('54_', ce.pk)  AS hv_lab_result_id,
    NULL                  AS crt_dt,
    NULL                  AS mdl_vrsn_num,
    ce.source_file_name   AS data_set_nm,
    NULL                  AS src_vrsn_id,
    NULL                  AS hvm_vdr_id,
    NULL                  AS hvm_vdr_feed_id,
    NULL                  AS vdr_org_id,
    NULL                  AS vdr_lab_test_id,
    NULL                  AS vdr_lab_test_id_qual,
    ce.pk                 AS vdr_lab_result_id,
    CASE
      WHEN ce.pk IS NOT NULL
      THEN 'VENDOR'
    END                   AS vdr_lab_result_id_qual,
    mp.hvid               AS hvid,
    mp.yearOfBirth        AS ptnt_birth_yr,
    ce.ageAtDiagnosis     AS ptnt_age_num,
    NULL                  AS ptnt_lvg_flg,
    NULL                  AS ptnt_dth_dt,
    mp.gender             AS ptnt_gender_cd,
    mp.state              AS ptnt_state_cd,
    mp.threeDigitZip      AS ptnt_zip3_cd,
    NULL                  AS hv_enc_id,
    NULL                  AS enc_dt,
    NULL                  AS hv_lab_ord_id,
    NULL                  AS lab_test_smpl_collctn_dt,
    NULL                  AS lab_test_schedd_dt,
    NULL                  AS lab_test_execd_dt,
    NULL                  AS lab_result_dt,
    NULL                  AS lab_test_ordg_prov_npi,
    NULL                  AS lab_test_ordg_prov_vdr_id,
    NULL                  AS lab_test_ordg_prov_vdr_id_qual,
    NULL                  AS lab_test_ordg_prov_alt_id,
    NULL                  AS lab_test_ordg_prov_alt_id_qual,
    NULL                  AS lab_test_ordg_prov_tax_id,
    NULL                  AS lab_test_ordg_prov_dea_id,
    NULL                  AS lab_test_ordg_prov_state_lic_id,
    NULL                  AS lab_test_ordg_prov_comrcl_id,
    NULL                  AS lab_test_ordg_prov_upin,
    NULL                  AS lab_test_ordg_prov_ssn,
    NULL                  AS lab_test_ordg_prov_nucc_taxnmy_cd,
    NULL                  AS lab_test_ordg_prov_alt_taxnmy_id,
    NULL                  AS lab_test_ordg_prov_alt_taxnmy_id_qual,
    NULL                  AS lab_test_ordg_prov_mdcr_speclty_cd,
    NULL                  AS lab_test_ordg_prov_alt_speclty_id,
    NULL                  AS lab_test_ordg_prov_alt_speclty_id_qual,
    NULL                  AS lab_test_ordg_prov_fclty_nm,
    NULL                  AS lab_test_ordg_prov_frst_nm,
    NULL                  AS lab_test_ordg_prov_last_nm,
    NULL                  AS lab_test_ordg_prov_addr_1_txt,
    NULL                  AS lab_test_ordg_prov_addr_2_txt,
    NULL                  AS lab_test_ordg_prov_state_cd,
    NULL                  AS lab_test_ordg_prov_zip_cd,
    NULL                  AS lab_test_exectg_fclty_npi,
    NULL                  AS lab_test_exectg_fclty_vdr_id,
    NULL                  AS lab_test_exectg_fclty_vdr_id_qual,
    NULL                  AS lab_test_exectg_fclty_alt_id,
    NULL                  AS lab_test_exectg_fclty_alt_id_qual,
    NULL                  AS lab_test_exectg_fclty_tax_id,
    NULL                  AS lab_test_exectg_fclty_dea_id,
    NULL                  AS lab_test_exectg_fclty_state_lic_id,
    NULL                  AS lab_test_exectg_fclty_comrcl_id,
    NULL                  AS lab_test_exectg_fclty_nucc_taxnmy_cd,
    NULL                  AS lab_test_exectg_fclty_alt_taxnmy_id,
    NULL                  AS lab_test_exectg_fclty_alt_taxnmy_id_qual,
    NULL                  AS lab_test_exectg_fclty_mdcr_speclty_cd,
    NULL                  AS lab_test_exectg_fclty_alt_speclty_id,
    NULL                  AS lab_test_exectg_fclty_alt_speclty_id_qual,
    NULL                  AS lab_test_exectg_fclty_nm,
    NULL                  AS lab_test_exectg_fclty_addr_1_txt,
    NULL                  AS lab_test_exectg_fclty_addr_2_txt,
    NULL                  AS lab_test_exectg_fclty_state_cd,
    NULL                  AS lab_test_exectg_fclty_zip_cd,
    NULL                  AS lab_test_specmn_typ_cd,
    NULL                  AS lab_test_fstg_stat_flg,
    NULL                  AS lab_test_panel_nm,
    NULL                  AS lab_test_nm,
    NULL                  AS lab_test_desc,
    NULL                  AS lab_test_loinc_cd,
    NULL                  AS lab_test_snomed_cd,
    TRIM(REGEXP_REPLACE(
        SPLIT(ce.breastcancertype, ',')[n],
        '[-+]', ''
        ))                AS lab_test_vdr_cd,
    'VENDOR'              AS lab_test_vdr_cd_qual,
    NULL                  AS lab_test_alt_cd,
    NULL                  AS lab_test_alt_cd_qual,
    CASE
      WHEN SPLIT(ce.breastcancertype, ',')[exploder.n] LIKE '%+'
      THEN 'Positive'
      WHEN SPLIT(ce.breastcancertype, ',')[exploder.n] LIKE '%-'
      THEN 'Negative'
    END                   AS lab_result_nm,
    NULL                  AS lab_result_desc,
    NULL                  AS lab_result_msrmt,
    NULL                  AS lab_result_uom,
    NULL                  AS lab_result_qual,
    NULL                  AS lab_result_abnorm_flg,
    NULL                  AS lab_result_norm_min_msrmt,
    NULL                  AS lab_result_norm_max_msrmt,
    parse_out_transmed_diagnosis(
        ce.primarysite
        )                 AS lab_test_diag_cd,
    NULL                  AS lab_test_diag_cd_qual,
    NULL                  AS data_captr_dt,
    NULL                  AS rec_stat_cd,
    'cancerepisode'       AS prmy_src_tbl_nm,
    {date_input}          AS date_input,
    SUBSTRING(
        {date_input}, 0, 7
        )                 AS part_mth
FROM transactions_cancerepisode ce
    LEFT JOIN matching_payload mp ON ce.patientfk = mp.claimid
    CROSS JOIN breast_cancer_type_exploder exploder
WHERE TRIM(SPLIT(ce.breastcancertype, ',')[exploder.n]) IS NOT NULL
    AND TRIM(SPLIT(ce.breastcancertype, ',')[exploder.n]) <> ''
