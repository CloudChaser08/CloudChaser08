SELECT
    CONCAT(
        '5_',
        COALESCE(
            SUBSTR(prc.date_performed, 1, 10),
            '0000-00-00'
        ),
        '_',
        prc.practice_key,
        '_',
        prc.patient_key
    )                                                                           AS hv_proc_id,
    CURRENT_DATE()                                                              AS crt_dt,
	'12'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_{VDR_FILE_DT}_',
        SPLIT(prc.input_file_name, '/')[SIZE(SPLIT(prc.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	CAST(NULL AS STRING) AS src_vrsn_id,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    prc.practice_key                                                            AS vdr_org_id,
    CAST(NULL AS STRING) AS vdr_proc_id,
    CAST(NULL AS STRING) AS vdr_proc_id_qual,
    CAST(NULL AS STRING) AS vdr_alt_proc_id,
    CAST(NULL AS STRING) AS vdr_alt_proc_id_qual,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(prc.date_performed, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(prc.date_performed, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_age_num,
    CAST(NULL AS STRING) AS ptnt_lvg_flg,
    CAST(NULL AS STRING) AS ptnt_dth_dt,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    CAST(NULL AS STRING) AS hv_enc_id,
    CAST(NULL AS DATE) AS enc_dt,
    EXTRACT_DATE(
        SUBSTR(prc.date_performed, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS proc_dt,
    CAST(NULL AS STRING) AS proc_prov_npi,
    CAST(NULL AS STRING) AS proc_prov_qual,
    CAST(NULL AS STRING) AS proc_prov_vdr_id,
    CAST(NULL AS STRING) AS proc_prov_vdr_id_qual,
    CAST(NULL AS STRING) AS proc_prov_alt_id,
    CAST(NULL AS STRING) AS proc_prov_alt_id_qual,
    CAST(NULL AS STRING) AS proc_prov_tax_id,
    CAST(NULL AS STRING) AS proc_prov_state_lic_id,
    CAST(NULL AS STRING) AS proc_prov_comrcl_id,
    CAST(NULL AS STRING) AS proc_prov_upin,
    CAST(NULL AS STRING) AS proc_prov_ssn,
    CAST(NULL AS STRING) AS proc_prov_nucc_taxnmy_cd,
    CAST(NULL AS STRING) AS proc_prov_alt_taxnmy_id,
    CAST(NULL AS STRING) AS proc_prov_alt_taxnmy_id_qual,
    CAST(NULL AS STRING) AS proc_prov_mdcr_speclty_cd,
    CAST(NULL AS STRING) AS proc_prov_alt_speclty_id,
    CAST(NULL AS STRING) AS proc_prov_alt_speclty_id_qual,
    CAST(NULL AS STRING) AS proc_prov_frst_nm,
    CAST(NULL AS STRING) AS proc_prov_last_nm,
    CAST(NULL AS STRING) AS proc_prov_fclty_nm,
    CAST(NULL AS STRING) AS proc_prov_addr_1_txt,
    CAST(NULL AS STRING) AS proc_prov_addr_2_txt,
    CAST(NULL AS STRING) AS proc_prov_state_cd,
    CAST(NULL AS STRING) AS proc_prov_zip_cd,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_npi,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_vdr_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_vdr_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_tax_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_state_lic_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_comrcl_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_nucc_taxnmy_cd,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_taxnmy_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_taxnmy_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_mdcr_speclty_cd,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_speclty_id,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_alt_speclty_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_nm,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_addr_1_txt,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_addr_2_txt,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_state_cd,
    CAST(NULL AS STRING) AS proc_rndrg_fclty_zip_cd,
    CAST(NULL AS STRING) AS proc_rndrg_prov_npi,
    prc.provider_key                                                            AS proc_rndrg_prov_vdr_id,
    CASE
        WHEN prc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS proc_rndrg_prov_vdr_id_qual,
    prc.practice_key                                                            AS proc_rndrg_prov_alt_id,
    CASE
        WHEN prc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS proc_rndrg_prov_alt_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_prov_tax_id,
    CAST(NULL AS STRING) AS proc_rndrg_prov_state_lic_id,
    CAST(NULL AS STRING) AS proc_rndrg_prov_comrcl_id,
    CAST(NULL AS STRING) AS proc_rndrg_prov_upin,
    CAST(NULL AS STRING) AS proc_rndrg_prov_ssn,
    CAST(NULL AS STRING) AS proc_rndrg_prov_nucc_taxnmy_cd,
    CAST(NULL AS STRING) AS proc_rndrg_prov_alt_taxnmy_id,
    CAST(NULL AS STRING) AS proc_rndrg_prov_alt_taxnmy_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_prov_mdcr_speclty_cd,
    prv.specialty                                                               AS proc_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS proc_rndrg_prov_alt_speclty_id_qual,
    CAST(NULL AS STRING) AS proc_rndrg_prov_frst_nm,
    CAST(NULL AS STRING) AS proc_rndrg_prov_last_nm,
    CAST(NULL AS STRING) AS proc_rndrg_prov_addr_1_txt,
    CAST(NULL AS STRING) AS proc_rndrg_prov_addr_2_txt,
    UPPER(COALESCE(prv.state, ''))                                              AS proc_rndrg_prov_state_cd,
    CAST(NULL AS STRING) AS proc_rndrg_prov_zip_cd,
    TRIM(UPPER(cpt.cpt_code))                                                   AS proc_cd,
    CASE
        WHEN cpt.cpt_code IS NULL THEN NULL
        ELSE 'CPT_CODE'
    END                                                                         AS proc_cd_qual,
    CAST(NULL AS STRING) AS proc_cd_1_modfr,
    CAST(NULL AS STRING) AS proc_cd_2_modfr,
    CAST(NULL AS STRING) AS proc_cd_3_modfr,
    CAST(NULL AS STRING) AS proc_cd_4_modfr,
    CAST(NULL AS STRING) AS proc_cd_modfr_qual,
    CAST(NULL AS STRING) AS proc_snomed_cd,
    CAST(NULL AS STRING) AS proc_prty_cd,
    CAST(NULL AS STRING) AS proc_prty_cd_qual,
    CAST(NULL AS STRING) AS proc_alt_cd,
    CAST(NULL AS STRING) AS proc_alt_cd_qual,
    CAST(NULL AS STRING) AS proc_ndc,
    CAST(NULL AS STRING) AS proc_pos_cd,
    prc.units                                                                   AS proc_unit_qty,
    CAST(NULL AS STRING) AS proc_uom,
    CAST(NULL AS STRING) AS proc_diag_cd,
    CAST(NULL AS STRING) AS proc_diag_cd_qual,
    CAST(NULL AS STRING) AS proc_stat_cd,
    CAST(NULL AS STRING) AS proc_stat_cd_qual,
    CAST(NULL AS STRING) AS proc_typ_cd,
    CAST(NULL AS STRING) AS proc_typ_cd_qual,
    CAST(NULL AS STRING) AS proc_admin_rte_cd,
    CAST(NULL AS STRING) AS proc_admin_site_cd,
    CAST(NULL AS STRING) AS proc_grp_txt,
    CAST(NULL AS STRING) AS data_src_cd,
    CAST(NULL AS STRING) AS data_captr_dt,
    CAST(NULL AS STRING) AS rec_stat_cd,
    'f_procedure'                                                               AS prmy_src_tbl_nm,
    '5'									                                        AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN 0 = LENGTH(
	    COALESCE(
            CAP_DATE(
                CAST(EXTRACT_DATE(SUBSTR(inj.date_given, 1, 10), '%Y-%m-%d') AS DATE),
                CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
                ),
                ''
            )
        ) THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(prc.date_performed, 1, 7)
	END                                                                         AS part_mth
FROM f_procedure prc
LEFT OUTER JOIN d_patient ptn ON prc.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON prc.provider_key = prv.provider_key
LEFT OUTER JOIN d_cpt cpt ON prc.cpt_key = cpt.cpt_key
WHERE
    TRIM(UPPER(COALESCE(prc.practice_key, 'empty'))) <> 'PRACTICE_KEY'
