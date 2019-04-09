SELECT
    '04'                                    AS mdl_vrsn_num,
    agy.dataset                             AS data_set_nm,
    agy.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        agy.reportingenterpriseid,
        agy.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        agy.reportingenterpriseid,
        agy.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(agy.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(agy.onsetdate, 1, 8), '%Y%m%d', NULL, CAST({max_date} AS DATE)
        )                                   AS clin_obsn_onset_dt,
    extract_date(
        substring(agy.resolveddate, 1, 8), '%Y%m%d', NULL, CAST({max_date} AS DATE)
        )                                   AS clin_obsn_resltn_dt,
    agy.allergentype                        AS clin_obsn_typ_cd,
    'ALLERGEN_TYPE_CODE'                    AS clin_obsn_typ_cd_qual,
    ref2.gen_ref_itm_nm                     AS clin_obsn_typ_nm,
    agy.allergentypedescription             AS clin_obsn_typ_desc,
    agy.allergencode                        AS clin_obsn_substc_cd,
    'ALLERGEN_CODE'                         AS clin_obsn_substc_cd_qual,
    ref1.gen_ref_itm_nm                     AS clin_obsn_substc_nm,
    agy.reportedsymptoms                    AS clin_obsn_desc,
    agy.intolerenceind                      AS clin_obsn_result_cd,
    'EXTREME_REACTION_INDICATOR'            AS clin_obsn_result_cd_qual,
    'allergy'                               AS prmy_src_tbl_nm,
    extract_date(
        substring(agy.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM allergy agy
    LEFT JOIN demographics_dedup dem ON agy.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND agy.NextGenGroupID = dem.NextGenGroupID
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'allergy.allergencode'
        AND agy.allergencode = ref1.gen_ref_cd
    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
        AND ref2.gen_ref_domn_nm = 'allergy.allergentype'
        AND agy.allergentype = ref2.gen_ref_cd
