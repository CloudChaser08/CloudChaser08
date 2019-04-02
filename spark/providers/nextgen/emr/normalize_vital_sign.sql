SELECT
    '04'                                    AS mdl_vrsn_num,
    vsn.dataset                             AS data_set_nm,
    vsn.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        vsn.reportingenterpriseid,
        vsn.nextgengroupid))                AS hvid,
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
        substring(vsn.vit_sign_last_msrmt_dt[x.n], 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS vit_sign_last_msrmt_dt,
    vsn.vit_sign_typ_cd[x.n]                AS vit_sign_typ_cd,
    vsn.vit_sign_msrmt[x.n]                 AS vit_sign_msrmt,
    vsn.vit_sign_uom[x.n]                   AS vit_sign_uom,
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
    CROSS JOIN vital_signs_exploder x
WHERE
(
    (
    -- Only keep a row if it's measurement is populated
        vsn.vit_sign_msrmt[x.n] IS NOT NULL
        AND vsn.vit_sign_msrmt[x.n] != ''
    )
    OR
    (
    -- Unless the vit_sign_msrmt is INCHES, we still want the row if the date exists
        vsn.vit_sign_last_msrmt_dt[x.n] IS NOT NULL
        AND vsn.vit_sign_uom[x.n] = 'INCHES'
    )
)
DISTRIBUTE BY hvid
