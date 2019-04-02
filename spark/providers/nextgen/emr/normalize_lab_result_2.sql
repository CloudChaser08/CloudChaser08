SELECT
    '04'                                    AS mdl_vrsn_num,
    lip.dataset                             AS data_set_nm,
    lip.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        lip.reportingenterpriseid,
        lip.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        lip.reportingenterpriseid,
        lip.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(lip.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    NULL                                    AS hv_lab_ord_id,
    NULL                                    AS lab_test_smpl_collctn_dt,
    extract_date(
        substring(lip.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS lab_test_execd_dt,
    extract_date(
        substring(lip.datadatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS lab_result_dt,
    'LIPID_PANEL'                           AS lab_test_panel_nm,
    ARRAY('LDL_CHOLESTEROL', 'HDL_CHOLESTEROL', 'TRIGLYCERIDES', 'TOTAL_CHOLESTEROL')[x.n]
                                            AS lab_test_nm,
    ARRAY('134577', '20859', '25718', '20933')[x.n]
                                            AS lab_test_loinc_cd,
    NULL                                    AS lab_test_snomed_cd,
    NULL                                    AS lab_test_vdr_cd,
    ARRAY(lip.ldl, lip.hdl, lip.triglycerides, lip.totalcholesterol)[x.n]
                                            AS lab_result_nm,
    NULL                                    AS lab_result_msrmt,
    ARRAY('mg/dl', 'mg/dl or mg/mL', 'mg/dl', 'mg/dl')[x.n]
                                            AS lab_result_uom,
    NULL                                    AS lab_result_qual,
    NULL                                    AS data_captr_dt,
    NULL                                    AS rec_stat_cd,
    'lipidpanel'                            AS prmy_src_tbl_nm,
    extract_date(
        substring(lip.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM lipidpanel lip
    LEFT JOIN demographics_local dem ON lip.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND lip.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(lip.encounterdate, 1, 8),
                substring(lip.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(lip.encounterdate, 1, 8),
                substring(lip.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    CROSS JOIN lipid_exploder x
WHERE
    ARRAY(lip.ldl, lip.hdl, lip.triglycerides,
            lip.totalcholesterol)[x.n]  IS NOT NULL
DISTRIBUTE BY hvid
