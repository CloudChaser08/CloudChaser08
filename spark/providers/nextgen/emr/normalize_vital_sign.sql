SELECT
    '04'                                    AS mdl_vrsn_num,
    vsn.dataset                             AS data_set_nm,
    vsn.reportingenterpriseid               AS vdr_org_id,
    concat_ws('_', 'NG',
        vsn.reportingenterpriseid,
        vsn.nextgengroupid) as hvid         AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        vsn.reportingenterpriseid,
        vsn.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(vsn.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(vsn.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS vit_sign_dt,
    extract_date(
        substring(split(vsn.vit_sign_last_msrmt_dt, ':')[explode_idx], 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS vit_sign_last_msrmt_dt,
    CASE WHEN split(vsn.vit_sign_typ_cd, ':')[explode_idx] = '' THEN NULL
        ELSE split(vsn.vit_sign_typ_cd, ':')[explode_idx]
        END                                 AS vit_sign_typ_cd,
    CASE WHEN vsn.vit_sign_msrmt[explode_idx] = '' THEN NULL
        ELSE vsn.vit_sign_msrmt[explode_idx]
        END                                 AS vit_sign_msrmt,
    CASE WHEN split(vsn.vit_sign_uom, ':')[explode_idx] = '' THEN NULL
        ELSE split(vsn.vit_sign_uom, ':')[explode_idx]
        END                                 AS vit_sign_uom,
    'vitalsigns'                            AS prmy_src_tbl_nm,
    extract_date(
        substring(vsn.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
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
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)) as explode_idx) x
WHERE (vsn.vit_sign_msrmt[explode_idx] IS NOT NULL AND vsn.vit_sign_msrmt[explode_idx] != '')
DISTRIBUTE BY hvid
