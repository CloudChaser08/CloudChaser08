SELECT
    concat_ws('_', '35', ord.reportingenterpriseid,
        ord.OrderNum)                       AS hv_lab_ord_id,
    '03'                                    AS mdl_vrsn_num,
    ord.dataset                             AS data_set_nm,
    ord.reportingenterpriseid               AS vdr_org_id,
    ord.ordernum                            AS vdr_lab_ord_id,
    'reportingenterpriseid'                 AS vdr_lab_ord_id_qual,
    concat_ws('_', 'NG',
        ord.reportingenterpriseid,
        ord.nextgengroupid) as hvid         AS hvid,
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
    NULL                                    AS prov_ord_dt,
    extract_date(
        substring(ord.scheduledtime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS lab_ord_test_schedd_dt,
    extract_date(
        substring(ord.collectiontime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS lab_ord_smpl_collctn_dt,
    translate(ord.loinccode, '-', '')       AS lab_ord_loinc_cd,
    clean_up_freetext(ord.snomedcode, false)
                                            AS lab_ord_snomed_cd
    clean_up_freetext(ord.testcodeid, false)
                                            AS lab_ord_alt_cd
    clean_up_freetext(ord.emrcode, false)   AS lab_ord_test_nm,
    trim(split(ord.diagnoses, ',')[explode_idx])
					    AS lab_ord_diag_cd,
    extract_date(
        substring(ord.datadate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS data_captr_dt,
    clean_up_freetext(ord.ngnstatus, false)
                                            AS rec_stat_cd,
    'laborder'                              AS prmy_src_tbl_nm,
    extract_date(
        substring(ord.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth
FROM laborder ord
    LEFT JOIN demographics_local dem ON ord.ReportingEnterpriseID = dem.ReportingEnterpriseID
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
    CROSS JOIN (SELECT explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)) as explode_idx) x
WHERE ((trim(split(ord.diagnoses, ',')[explode_idx]) IS NOT NULL AND trim(split(ord.diagnoses, ',')[explode_idx]) != '')
    	OR (explode_idx = 0 AND regexp_extract(ord.diagnoses, '([^,\\s])') IS NULL))
DISTRIBUTE BY hvid
