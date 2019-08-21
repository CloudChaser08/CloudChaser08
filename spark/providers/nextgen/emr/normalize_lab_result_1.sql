SELECT
    '08'                                    AS mdl_vrsn_num,
    rslt.dataset                            AS data_set_nm,
    rslt.reportingenterpriseid              AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        rslt.reportingenterpriseid,
        rslt.nextgengroupid))               AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        rslt.reportingenterpriseid,
        rslt.encounter_id)                  AS hv_enc_id,
    extract_date(
        substring(rslt.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    CASE WHEN rslt.ordernum IS NOT NULL THEN concat_ws('_', '35',
            rslt.reportingenterpriseid,
            rslt.ordernum)
        ELSE NULL END                       AS hv_lab_ord_id,
    extract_date(
        substring(rslt.collectiontime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS lab_test_smpl_collctn_dt,
    NULL                                    AS lab_test_execd_dt,
    NULL                                    AS lab_result_dt,
    NULL                                    AS lab_test_panel_nm,
    CASE WHEN translate(rslt.emrcode, '-', '') = clean_up_numeric_code(rslt.emrcode)
            THEN translate(rslt.emrcode, '-', '')
        ELSE ref.gen_ref_itm_nm END         AS lab_test_nm,
    translate(rslt.loinccode, '-', '')      AS lab_test_loinc_cd,
    clean_up_freetext(rslt.snomedcode, false)
                                            AS lab_test_snomed_cd,
    clean_up_freetext(rslt.testcodeid, false)
                                            AS lab_test_vdr_cd,
    CASE WHEN CAST(rslt.result as float) IS NOT NULL
            THEN rslt.result
        ELSE ref2.gen_ref_itm_nm END        AS lab_result_nm,
    rslt.referencerange                     AS lab_result_msrmt,
    rslt.unitofmeasure                      AS lab_result_uom,
    CASE
        WHEN rslt.referencerange IS NULL
            THEN NULL
        ELSE 'REFERENCERANGE'
    END                                     AS lab_result_qual,
    substring(rslt.normalabnormalflag, 1, 1) AS lab_result_abnorm_flg,
    extract_date(
        substring(rslt.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS data_captr_dt,
    clean_up_freetext(rslt.ngnstatus, false)
                                            AS rec_stat_cd,
    'labresult'                             AS prmy_src_tbl_nm,
    extract_date(
        substring(rslt.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM labresult rslt
    LEFT JOIN demographics_local dem ON rslt.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND rslt.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(rslt.encounterdate, 1, 8),
                substring(rslt.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(rslt.encounterdate, 1, 8),
                substring(rslt.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_lab_result.lab_test_nm'
                AND whtlst_flg = 'Y'
        ) ref
        ON TRIM(UPPER(rslt.emrcode)) = ref.gen_ref_itm_nm
    LEFT JOIN (SELECT DISTINCT gen_ref_itm_nm
            FROM ref_gen_ref
            WHERE gen_ref_domn_nm = 'emr_lab_result.lab_result_nm'
            AND whtlst_flg = 'Y'
        ) ref2
        ON TRIM(UPPER(rslt.result)) = ref2.gen_ref_itm_nm
