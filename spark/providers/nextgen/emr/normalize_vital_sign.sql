DROP TABLE IF EXISTS vitalsigns_w_msrmt;
CREATE TABLE vitalsigns_w_msrmt AS
SELECT *,
    concat_ws(':',
        'SYSTOLIC',
        'DIASTOLIC',
        'PULSE',
        'BMI',
        'BMI',
        'O2_SATURATION',
        'O2_SATURATION',
        'RESPIRATION_FLOW',
        'RESPIRATION_FLOW',
        'BODY_TEMPERATURE',
        'RESPIRATION',
        'STANFORD_HAQ',
        'PAIN',
        'HEIGHT',
        'WEIGHT'
    ) as vit_sign_typ_cd,
    concat_ws(':',
        'mmHg',
        'mmHg',
        'BEATS_PER_MINUTE',
        'INDEX',
        'PERCENT',
        'PERCENT',
        'TIMING',
        'RATE',
        'TIMING',
        'FAHRENHEIT',
        'BREATHS_PER_MINUTE',
        'SCORE',
        '0_THROUGH_10',
        'INCHES',
        'POUNDS'
    ) as vit_sign_uom,
    concat('::::::::::::', COALESCE(vsn.heightdate, ''), ':') as vit_sign_last_msrmt_dt,
    array(
        vsn.systolic,
        vsn.diastolic,
        vsn.pulserate,
        vsn.bmi,
        vsn.bmipercent,
        vsn.spo2dtl,
        vsn.spo2timingid,
        vsn.peakflow,
        vsn.peakflowtiming,
        vsn.tempdegf,
        vsn.respirationrate,
        vsn.haqscore,
        vsn.pain,
        CASE WHEN vsn.heightft IS NOT NULL
                THEN extract_number(vsn.heightft) * 12 + extract_number(vsn.heightin)
            WHEN vsn.heightcm IS NOT NULL
                THEN floor(extract_number(vsn.heightcm) * 2.54)
            END,
        CASE WHEN vsn.weightlb IS NOT NULL
                THEN extract_number(vsn.weightlb)
            WHEN vsn.weightkg IS NOT NULL
                THEN floor(extract_number(vsn.weightkg) * 2.2)
            END
    ) as vit_sign_msrmt
FROM vitalsigns vsn;

INSERT INTO vital_sign_common_model
SELECT
    NULL,                                   -- row_id
    NULL,                                   -- hv_vit_sign_id
    NULL,                                   -- crt_dt
    '04',                                   -- mdl_vrsn_num
    vsn.dataset,                            -- data_set_nm
    NULL,                                   -- src_vrsn_id
    NULL,                                   -- hvm_vdr_id
    NULL,                                   -- hvm_vdr_feed_id
    vsn.reportingenterpriseid,              -- vdr_org_id
    NULL,                                   -- vdr_clin_obsn_id
    NULL,                                   -- vdr_clin_obsn_id_qual
    concat_ws('_', 'NG',
        vsn.reportingenterpriseid,
        vsn.nextgengroupid) as hvid,        -- hvid
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
        vsn.reportingenterpriseid,
        vsn.encounter_id),                  -- hv_enc_id
    extract_date(
        substring(vsn.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- enc_dt
    extract_date(
        substring(vsn.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- vit_sign_dt
    extract_date(
        substring(split(vsn.vit_sign_last_msrmt_dt, ':')[explode_idx], 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        ),                                  -- vit_sign_last_msrmt_dt
    NULL,                                   -- vit_sign_rndrg_fclty_npi
    NULL,                                   -- vit_sign_rndrg_fclty_vdr_id
    NULL,                                   -- vit_sign_rndrg_fclty_vdr_id_qual
    NULL,                                   -- vit_sign_rndrg_fclty_alt_id
    NULL,                                   -- vit_sign_rndrg_fclty_alt_id_qual
    NULL,                                   -- vit_sign_rndrg_fclty_tax_id
    NULL,                                   -- vit_sign_rndrg_fclty_dea_id
    NULL,                                   -- vit_sign_rndrg_fclty_state_lic_id
    NULL,                                   -- vit_sign_rndrg_fclty_comrcl_id
    NULL,                                   -- vit_sign_rndrg_fclty_nucc_taxnmy_cd
    NULL,                                   -- vit_sign_rndrg_fclty_alt_taxnmy_id
    NULL,                                   -- vit_sign_rndrg_fclty_alt_taxnmy_id_qual
    NULL,                                   -- vit_sign_rndrg_fclty_mdcr_speclty_cd
    NULL,                                   -- vit_sign_rndrg_fclty_alt_speclty_id
    NULL,                                   -- vit_sign_rndrg_fclty_alt_speclty_id_qual
    NULL,                                   -- vit_sign_rndrg_fclty_nm
    NULL,                                   -- vit_sign_rndrg_fclty_addr_1_txt
    NULL,                                   -- vit_sign_rndrg_fclty_addr_2_txt
    NULL,                                   -- vit_sign_rndrg_fclty_state_cd
    NULL,                                   -- vit_sign_rndrg_fclty_zip_cd
    NULL,                                   -- vit_sign_rndrg_prov_npi
    NULL,                                   -- vit_sign_rndrg_prov_vdr_id
    NULL,                                   -- vit_sign_rndrg_prov_vdr_id_qual
    NULL,                                   -- vit_sign_rndrg_prov_alt_id
    NULL,                                   -- vit_sign_rndrg_prov_alt_id_qual
    NULL,                                   -- vit_sign_rndrg_prov_tax_id
    NULL,                                   -- vit_sign_rndrg_prov_dea_id
    NULL,                                   -- vit_sign_rndrg_prov_state_lic_id
    NULL,                                   -- vit_sign_rndrg_prov_comrcl_id
    NULL,                                   -- vit_sign_rndrg_prov_upin
    NULL,                                   -- vit_sign_rndrg_prov_ssn
    NULL,                                   -- vit_sign_rndrg_prov_nucc_taxnmy_cd
    NULL,                                   -- vit_sign_rndrg_prov_alt_taxnmy_id
    NULL,                                   -- vit_sign_rndrg_prov_alt_taxnmy_id_qual
    NULL,                                   -- vit_sign_rndrg_prov_mdcr_speclty_cd
    NULL,                                   -- vit_sign_rndrg_prov_alt_speclty_id
    NULL,                                   -- vit_sign_rndrg_prov_alt_speclty_id_qual
    NULL,                                   -- vit_sign_rndrg_prov_frst_nm
    NULL,                                   -- vit_sign_rndrg_prov_last_nm
    NULL,                                   -- vit_sign_rndrg_prov_addr_1_txt
    NULL,                                   -- vit_sign_rndrg_prov_addr_2_txt
    NULL,                                   -- vit_sign_rndrg_prov_state_cd
    NULL,                                   -- vit_sign_rndrg_prov_zip_cd
    CASE WHEN split(vsn.vit_sign_typ_cd, ':')[explode_idx] = '' THEN NULL
        ELSE split(vsn.vit_sign_typ_cd, ':')[explode_idx]
        END,                                -- vit_sign_typ_cd
    NULL,                                   -- vit_sign_typ_cd_qual
    NULL,                                   -- vit_sign_typ_nm
    NULL,                                   -- vit_sign_typ_desc
    NULL,                                   -- vit_sign_snomed_cd
    CASE WHEN vsn.vit_sign_msrmt[explode_idx] = '' THEN NULL
        ELSE vsn.vit_sign_msrmt[explode_idx]
        END,                                -- vit_sign_msrmt
    CASE WHEN split(vsn.vit_sign_uom, ':')[explode_idx] = '' THEN NULL
        ELSE split(vsn.vit_sign_uom, ':')[explode_idx]
        END,                                -- vit_sign_uom
    NULL,                                   -- vit_sign_qual
    NULL,                                   -- vit_sign_abnorm_flg
    NULL,                                   -- vit_sign_norm_min_msrmt
    NULL,                                   -- vit_sign_norm_max_msrmt
    NULL,                                   -- data_captr_dt
    NULL,                                   -- rec_stat_cd
    'vitalsigns',                           -- prmy_src_tbl_nm
    extract_date(
        substring(vsn.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   -- part_mth
FROM vitalsigns_w_msrmt vsn
    LEFT JOIN demographics_local dem ON vsn.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND vsn.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(vsn.encounterdate, 1, 8),
                substring(vsn.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(vsn.encounterdate, 1, 8),
                substring(vsn.referencedatetime, 1, 8)
            ) <= substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)) as explode_idx) x
WHERE (vsn.vit_sign_msrmt[explode_idx] IS NOT NULL AND vsn.vit_sign_msrmt[explode_idx] != '')
DISTRIBUTE BY hvid;
