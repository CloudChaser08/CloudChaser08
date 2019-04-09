SELECT
    '04'                                    AS mdl_vrsn_num,
    med.dataset                             AS data_set_nm,
    med.reportingenterpriseid               AS vdr_org_id,
    COALESCE(dem.hvid, concat_ws('_', '118',
        med.reportingenterpriseid,
        med.nextgengroupid))                AS hvid,
    dem.birthyear                           AS ptnt_birth_yr,
    CASE WHEN dem.gender = 'M' THEN 'M'
        WHEN dem.gender = 'F' THEN 'F'
        ELSE 'U' END                        AS ptnt_gender_cd,
    dem.zip3                                AS ptnt_zip3_cd,
    concat_ws('_', '35',
        med.reportingenterpriseid,
        med.encounter_id)                   AS hv_enc_id,
    extract_date(
        substring(med.encounterdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS enc_dt,
    extract_date(
        substring(med.orderdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS medctn_ord_dt,
    extract_date(
        substring(med.startdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS medctn_admin_dt,
    extract_date(
        substring(med.startdate, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS medctn_start_dt,
    extract_date(
        substring(med.datestopped, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS medctn_end_dt,
    clean_up_freetext(trim(split(med.diagnosis_code_id, ',')[x.n]), true)
                                            AS medctn_diag_cd,
    med.emrcode                             AS medctn_ndc,
    med.hiclsqno                            AS medctn_hicl_thrptc_cls_cd,
    med.hic3                                AS medctn_hicl_cd,
    med.gcnseqno                            AS medctn_gcn_cd,
    med.rxnorm                              AS medctn_rxnorm_cd,
    CASE WHEN med.med_class_id = 'O' THEN 'N'
        WHEN med.med_class_id = 'F' THEN 'Y'
        ELSE NULL END                       AS medctn_rx_flg,
    med.rxquantity                          AS medctn_rx_qty,
    clean_up_freetext(med.sigcodes, false)  AS medctn_admin_sig_cd,
    med.dose                                AS medctn_dose_txt,
    med.orgrefills                          AS medctn_fll_num,
    med.rxrefills                           AS medctn_remng_rfll_qty,
    extract_date(
        substring(med.datelastrefilled, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS medctn_last_rfll_dt,
    'medicationorder'                       AS prmy_src_tbl_nm,
    extract_date(
        substring(med.referencedatetime, 1, 8), '%Y%m%d', CAST({min_date} AS DATE), CAST({max_date} AS DATE)
        )                                   AS part_mth,
    row_num                                 AS row_num
FROM medicationorder med
    LEFT JOIN demographics_local dem ON med.ReportingEnterpriseID = dem.ReportingEnterpriseID
        AND med.NextGenGroupID = dem.NextGenGroupID
        AND COALESCE(
                substring(med.encounterdate, 1, 8),
                substring(med.referencedatetime, 1, 8)
            ) >= substring(dem.recorddate, 1, 8)
        AND (COALESCE(
                substring(med.encounterdate, 1, 8),
                substring(med.referencedatetime, 1, 8)
            ) < substring(dem.nextrecorddate, 1, 8)
            OR dem.nextrecorddate IS NULL)
    CROSS JOIN medication_exploder x
WHERE ((split(med.diagnosis_code_id, ',')[x.n] IS NOT NULL
            AND trim(split(med.diagnosis_code_id, ',')[x.n]) != '')
        OR (x.n = 0 AND regexp_extract(med.diagnosis_code_id, '([^,\\s])') IS NULL))
DISTRIBUTE BY hvid
