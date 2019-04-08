SELECT
    '09'                                    AS mdl_vrsn_num,
    ord.dataset                             AS data_set_nm,
    ord.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        ord.reportingenterpriseid,
        ord.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        ord.reportingenterpriseid,
        ord.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(ord.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(ord.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS proc_dt,
    ord.orderinghcpprimarytaxonomy          AS proc_rndrg_prov_nucc_taxnmy_cd,
    ord.orderinghcpzipcode                  AS proc_rndrg_prov_zip_cd,
    ARRAY(
        ord.real_vcxcode,
        ord.real_actcode
    )[e.n]                                  AS proc_cd,
    TRIM(UPPER(
        ARRAY(
            'CVX',
            'ACTCODE'
        )[e.n]
    ))                                      AS proc_cd_qual,
    ord.clean_actdiagnosiscode              AS proc_diag_cd,
    TRIM(UPPER(ord.actstatus))              AS proc_stat_cd,
    CASE
        WHEN ord.actstatus IS NOT NULL AND LENGTH(TRIM(ord.actstatus)) <> 0 THEN 'ACTSTATUS'
        ELSE NULL
    END                                     AS proc_stat_cd_qual,
    ord.actclass                            AS proc_typ_cd,
    CASE
        WHEN ord.actclass IS NOT NULL AND LENGTH(TRIM(ord.actclass)) <> 0 THEN 'ACTCLASS'
        ELSE NULL
    END                                     AS proc_typ_cd_qual,
    'order'                                 AS prmy_src_tbl_nm
FROM `procedure_order_real_actcode` ord
    LEFT JOIN demographics_local dem 
    CROSS JOIN procedure_2_exploder e
    ON ord.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND ord.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(ord.encounterdate, 1, 8),
                substring(ord.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(ord.encounterdate, 1, 8),
                substring(ord.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'order.actmood'
        AND ord.actmood = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y'
    LEFT JOIN ref_gen_ref ref2 ON ref2.hvm_vdr_feed_id = 35
        AND ref2.gen_ref_domn_nm = 'order.actclass'
        AND ord.actclass = ref2.gen_ref_cd
        AND ref2.whtlst_flg = 'Y'
    -- Only take rows where vcxcode is not null
    WHERE ord.real_vcxcode IS NOT NULL
        AND ARRAY(ord.real_vcxcode, ord.real_actcode)[e.n] IS NOT NULL
