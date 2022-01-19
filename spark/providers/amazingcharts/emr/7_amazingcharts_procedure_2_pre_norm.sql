SELECT
    CONCAT(
        '5_',
        COALESCE(
            SUBSTR(inj.date_given, 1, 10),
            '0000-00-00'
        ),
        '_',
        inj.practice_key,
        '_',
        inj.patient_key
    )                                                                           AS hv_proc_id,
    CURRENT_DATE()                                                              AS crt_dt,
	'08'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_','{VDR_FILE_DT}', '_' ,
        SPLIT(inj.input_file_name, '/')[SIZE(SPLIT(inj.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
    CAST(NULL AS STRING) AS src_vrsn_id,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    inj.practice_key                                                            AS vdr_org_id,
    CAST(NULL AS STRING) AS vdr_proc_id,
    CAST(NULL AS STRING) AS vdr_proc_id_qual,
    CAST(NULL AS STRING) AS vdr_alt_proc_id,
    CAST(NULL AS STRING) AS vdr_alt_proc_id_qual,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(inj.date_given, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(EXTRACT_DATE(SUBSTR(inj.date_given, 1, 10), '%Y-%m-%d') AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
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
        SUBSTR(inj.date_given, 1, 10),
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
    inj.provider_key                                                            AS proc_rndrg_prov_vdr_id,
    CASE
        WHEN inj.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS proc_rndrg_prov_vdr_id_qual,
    prv.practice_key                                                            AS proc_rndrg_prov_alt_id,
    CASE
        WHEN prv.practice_key IS NULL THEN NULL
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
    ARRAY(
        UPPER(inj.cpt),
        UPPER(vcx.cpt_code)
    )[proc_2_exploder.n]                                                        AS proc_cd,
    ARRAY(
        CASE
            WHEN inj.cpt IS NULL THEN NULL
            ELSE 'F_INJECTION.CPT'
        END,
        CASE
            WHEN vcx.cpt_code IS NULL THEN NULL
            ELSE 'D_VACCINE_CPT.CPT_CODE'
        END
    )[proc_2_exploder.n]                                                        AS proc_cd_qual,
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
    REGEXP_REPLACE(
        inj.volume,
        '[^0-9|\.]',
        ''
    )                                                                           AS proc_unit_qty,
    REGEXP_REPLACE(
        inj.volume,
        '[0-9|\.]',
        ''
    )                                                                           AS proc_uom,
    CAST(NULL AS STRING) AS proc_diag_cd,
    CAST(NULL AS STRING) AS proc_diag_cd_qual,
    CASE
        WHEN inj.patient_refused = 'True' OR inj.patient_refused = '1' THEN 'Patient Refused'
        WHEN inj.patient_parent_refused = 'True' OR inj.patient_parent_refused = '1' THEN 'Patient Parent Refused'
        ELSE NULL
    END                                                                         AS proc_stat_cd,
    CASE
        WHEN inj.patient_refused = 'True' OR inj.patient_refused = '1' THEN 'INJECTION_REFUSED'
        WHEN inj.patient_parent_refused = 'True' OR inj.patient_parent_refused = '1' THEN 'INJECTION_REFUSED'
        ELSE NULL
    END                                                                         AS proc_stat_cd_qual,
    inj.record_type                                                             AS proc_typ_cd,
    CASE
        WHEN inj.record_type IS NULL THEN NULL
        ELSE 'RECORD_TYPE'
    END                                                                         AS proc_typ_cd_qual,
    inj.route                                                                   AS proc_admin_rte_cd,
    inj.site                                                                    AS proc_admin_site_cd,
    CAST(NULL AS STRING) AS proc_grp_txt,
    CAST(NULL AS STRING) AS data_src_cd,
    CAST(NULL AS STRING) AS data_captr_dt,
    CASE
        WHEN inj.deleted = 'True' OR inj.deleted = '1' THEN 'DELETED'
    END                                                                         AS rec_stat_cd,
    'f_injection'                                                               AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------

    CASE
	    WHEN CAP_DATE
	            (
                    CAST(EXTRACT_DATE(SUBSTR(inj.date_given, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(inj.date_given, 1, 7)
	END                                                                         AS part_mth

FROM f_injection inj
    LEFT OUTER JOIN d_patient ptn ON inj.patient_key = ptn.patient_key
    LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
    LEFT OUTER JOIN d_provider prv ON inj.provider_key = prv.provider_key
    LEFT OUTER JOIN d_vaccine_cpt vcx ON inj.vaccine_cpt_key = vcx.vaccine_cpt_key
    INNER JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) proc_2_exploder
WHERE
    TRIM(UPPER(COALESCE(inj.practice_key, 'empty'))) <> 'PRACTICE_KEY'
    AND
--- Only keep a row if it's proc_cd is not null
    (
        ARRAY(inj.cpt, vcx.cpt_code)[proc_2_exploder.n] IS NOT NULL
    )
    OR
--- If inj.cpt and vcx.cpt_code is null, keep one row that has a NULL proc_cd
    (
        COALESCE(inj.cpt, vcx.cpt_code) IS NULL
        AND proc_2_exploder.n = 0
    )
-- LIMIT 100
