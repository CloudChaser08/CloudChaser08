SELECT
    '05'                                    AS mdl_vrsn_num,
    diag.dataset                            AS data_set_nm,
    diag.reportingenterpriseid              AS vdr_org_id,
    concat_ws('_', 'NG',
        diag.reportingenterpriseid,
        diag.nextgengroupid)                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        diag.reportingenterpriseid,
        diag.encounter_id)                  AS hv_enc_id,
    extract_date(
        substring(diag.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(diag.diagnosisdate, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS diag_dt,
    extract_date(
        substring(diag.onsetdate, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS diag_onset_dt,
    extract_date(
        substring(diag.dateresolved, 1, 8), '%Y%m%d', CAST({diag_min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS diag_resltn_dt,
    diag.emrcode                            AS diag_cd,
    diag.dxpriority                         AS diag_svty_cd,
    clean_up_numeric_code(diag.statusid)    AS diag_stat_cd,
    CASE WHEN clean_up_numeric_code(diag.statusid) IS NOT NULL
            THEN 'VENDOR'
        END                                 AS diag_stat_cd_qual,
    ref1.gen_ref_itm_nm                     AS diag_stat_nm,
    clean_up_freetext(diag.statusidtext, false)
                                            AS diag_stat_desc,
    'diagnosis'                             AS prmy_src_tbl_nm
FROM diagnosis diag
    LEFT JOIN demographics_local dem ON diag.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND diag.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(diag.encounterdate, 1, 8),
                substring(diag.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(diag.encounterdate, 1, 8),
                substring(diag.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'diagnosis.statusid'
        AND diag.statusid = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y'
