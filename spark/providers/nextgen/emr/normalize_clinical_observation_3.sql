SELECT
    '04'                                    AS mdl_vrsn_num,
    ext.dataset                             AS data_set_nm,
    ext.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        ext.reportingenterpriseid,
        ext.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        ext.reportingenterpriseid,
        ext.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(ext.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(ext.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS clin_obsn_dt,
    ref2.gen_ref_cd                         AS clin_obsn_data_ctgy_cd,
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END                                 AS clin_obsn_data_ctgy_cd_qual,
    ref2.gen_ref_itm_nm                     AS clin_obsn_data_ctgy_nm,
    clean_up_freetext(ext.clinicalrecordtypecode, false)
                                            AS clin_obsn_typ_cd,
    'VENDOR'                                AS clin_obsn_typ_cd_qual,
    clean_up_freetext(ext.clinicalrecorddescription, false)
                                            AS clin_obsn_typ_nm,
    NULL                                    AS clin_obsn_substc_cd,
    NULL                                    AS clin_obsn_substc_cd_qual,
    NULL                                    AS clin_obsn_substc_nm,
    CASE WHEN CAST(ext.emrcode AS DOUBLE) IS NOT NULL THEN ext.emrcode
        ELSE ref3.gen_ref_itm_nm END        AS clin_obsn_nm,
    CASE WHEN CAST(ext.result AS DOUBLE) IS NOT NULL THEN ext.result
        ELSE ref4.gen_ref_itm_nm END        AS clin_obsn_result_desc,
    'extendeddata'                          AS prmy_src_tbl_nm,
    extract_date(
        substring(ext.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM extendeddata ext
    LEFT JOIN demographics_local dem ON ext.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND ext.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(ext.encounterdate, 1, 8),
                substring(ext.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(ext.encounterdate, 1, 8),
                substring(ext.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref2
        ON hvm_vdr_feed_id = 35
        AND gen_ref_domn_nm = 'extendeddata.datacategory'
        AND whtlst_flg = 'Y'
        AND ext.datacategory = ref2.gen_ref_cd
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_nm'
                AND whtlst_flg = 'Y'
        ) ref3
        ON TRIM(UPPER(ext.emrcode)) = ref3.gen_ref_itm_nm
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_result_desc'
                AND whtlst_flg = 'Y'
        ) ref4
        ON TRIM(UPPER(ext.result)) = ref4.gen_ref_itm_nm
