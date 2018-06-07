SELECT
    '04'                                    AS mdl_vrsn_num,
    sub.dataset                             AS data_set_nm,
    sub.reportingenterpriseid               AS vdr_org_id,
    concat_ws('_', 'NG',
        sub.reportingenterpriseid,
        sub.nextgengroupid)                 AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        sub.reportingenterpriseid,
        sub.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(sub.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(sub.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS clin_obsn_dt,
    ref2.gen_ref_cd                         AS clin_obsn_typ_cd,
    CASE WHEN ref2.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END                                 AS clin_obsn_typ_cd_qual,
    ref3.gen_ref_itm_nm                     AS clin_obsn_typ_nm,
    ref1.gen_ref_cd                         AS clin_obsn_substc_cd,
    CASE WHEN ref1.gen_ref_cd IS NOT NULL THEN 'VENDOR'
        END                                 AS clin_obsn_substc_cd_qual,
    ref1.gen_ref_itm_nm                     AS clin_obsn_substc_nm,
    CASE WHEN CAST(sub.emrcode AS DOUBLE) IS NOT NULL THEN sub.emrcode
        ELSE ref4.gen_ref_itm_nm END        AS clin_obsn_nm,
    'substanceusage'                        AS prmy_src_tbl_nm,
    extract_date(
        substring(sub.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM substanceusage sub
    LEFT JOIN demographics_local dem ON sub.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND sub.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(sub.encounterdate, 1, 8),
                substring(sub.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(sub.encounterdate, 1, 8),
                substring(sub.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN ref_gen_ref ref1 ON ref1.hvm_vdr_feed_id = 35
        AND ref1.gen_ref_domn_nm = 'substanceusage.substancecode'
        AND sub.substancecode = ref1.gen_ref_cd
        AND ref1.whtlst_flg = 'Y'
    LEFT JOIN (SELECT DISTINCT gen_ref_cd
            FROM ref_gen_ref
            WHERE hvm_vdr_feed_id = 35
                AND gen_ref_domn_nm = 'substanceusage.clinicalrecordtypecode'
                AND whtlst_flg = 'Y'
        ) ref2
        ON clean_up_freetext(sub.clinicalrecordtypecode, false) = ref2.gen_ref_cd
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
            FROM ref_gen_ref
            WHERE hvm_vdr_feed_id = 35
                AND gen_ref_domn_nm = 'substanceusage.clinicalrecorddescription'
                AND whtlst_flg = 'Y'
        ) ref3
        ON clean_up_freetext(sub.clinicalrecorddescription, false) = ref3.gen_ref_itm_nm
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
        FROM ref_gen_ref
        WHERE gen_ref_domn_nm = 'emr_clin_obsn.clin_obsn_nm'
            AND whtlst_flg = 'Y'
        ) ref4
        ON TRIM(UPPER(sub.emrcode)) = ref4.gen_ref_itm_nm
