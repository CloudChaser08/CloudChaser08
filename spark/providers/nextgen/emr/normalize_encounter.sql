SELECT
    concat_ws('_', '35',
        enc.reportingenterpriseid,
        enc.encounterid)                    AS hv_enc_id,
    '09'                                    AS mdl_vrsn_num,
    enc.dataset                             AS data_set_nm,
    enc.reportingenterpriseid               AS vdr_org_id,
    enc.encounterid                         AS vdr_enc_id,
    CASE WHEN enc.encounterid IS NOT NULL THEN 'VENDOR'
        END                                 AS vdr_enc_id_qual,
    COALESCE(dem.hvid, concat_ws('_', '118',
        enc.reportingenterpriseid,
        enc.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    extract_date(
        substring(enc.encounterdatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_start_dt,
    CASE
        WHEN LENGTH(REGEXP_REPLACE(enc.renderinghcpnpi, '[^0-9]', '')) == 10
        THEN REGEXP_REPLACE(enc.renderinghcpnpi, '[^0-9]', '')
        ELSE ''
    END AS enc_rndrg_prov_npi,
    enc.hcpprimarytaxonomy                  AS enc_rndrg_prov_nucc_taxnmy_cd,
    enc.hcpzipcode                          AS enc_rndrg_prov_zip_cd,
    UPPER(clean_up_freetext(enc.encounterdescription))
                                            AS enc_typ_nm,
    'encounter'                             AS prmy_src_tbl_nm
FROM encounter_dedup enc
    LEFT JOIN demographics_local dem ON enc.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND enc.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(enc.encounterdatetime, 1, 8),
                substring(enc.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(enc.encounterdatetime, 1, 8),
                substring(enc.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
