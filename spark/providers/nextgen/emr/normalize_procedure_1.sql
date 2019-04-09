SELECT
    '09'                                    AS mdl_vrsn_num,
    proc.dataset                            AS data_set_nm,
    proc.reportingenterpriseid              AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        proc.reportingenterpriseid,
        proc.nextgengroupid))               AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        proc.reportingenterpriseid,
        proc.encounter_id)                  AS hv_enc_id,
    extract_date(
        substring(proc.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(proc.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS proc_dt,
    proc.emrcode                            AS proc_cd,
    CASE WHEN proc.emrcode IS NOT NULL THEN 'CPT'
        END                                 AS proc_cd_qual,
    'procedure'                             AS prmy_src_tbl_nm
FROM `procedure` proc
    LEFT JOIN demographics_local dem ON proc.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND proc.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(proc.encounterdate, 1, 8),
                substring(proc.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(proc.encounterdate, 1, 8),
                substring(proc.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
