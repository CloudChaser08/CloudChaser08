SELECT
    CONCAT('25_', res.gen2patientid, '_', res.resultid, '_', res.versionid)    AS hv_lab_result_id,
    res.rectypeversion                                                         AS src_vrsn_id,
    res.genclientid                                                            AS vdr_org_id,
    res.primarykey                                                             AS vdr_lab_test_id,
    CASE WHEN res.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_lab_test_id_qual,
    res.primarykey                                                             AS vdr_lab_result_id,
    CASE WHEN res.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_lab_result_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', res.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    CONCAT('25_', res.gen2patientid, '_', res.encounterid)                     AS hv_enc_id,
    enc.encounterdttm                                                          AS enc_dt,
    CONCAT('25_', res.gen2patientid, '_', res.orderid)                         AS hv_lab_ord_id,
    res.performeddttm                                                          AS lab_test_execd_dt,
    res.resultdttm                                                             AS lab_result_dt,
    res.gen2providerid                                                         AS lab_test_ordg_prov_vdr_id,
    CASE WHEN res.gen2providerid IS NOT NULL THEN  'GEN2PROVIDERID' END        AS lab_test_ordg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS lab_test_ordg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS lab_test_ordg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS lab_test_ordg_prov_alt_speclty_id_qual,
    prv.state                                                                  AS lab_test_ordg_prov_state_cd,
    res.panel                                                                  AS lab_test_panel_nm,
    res.test                                                                   AS lab_test_nm,
    res.loinc                                                                  AS lab_test_loinc_cd,
    res.ocdid                                                                  AS lab_test_vdr_cd,
    CASE WHEN res.ocdid IS NOT NULL THEN 'OCDID' END                           AS lab_test_vdr_cd_qual,
    CASE WHEN rbf.resultid IS NOT NULL
        THEN rbf.value ELSE res.value END                                      AS lab_result_nm,
    CASE WHEN rbf.resultid IS NOT NULL
        THEN rbf.units ELSE res.units END                                      AS lab_result_uom,
    CASE WHEN TRIM(UPPER(res.abnormalflag)) IN ('N', 'Y')
    THEN TRIM(UPPER(res.abnormalflag))
    END                                                                        AS lab_result_abnorm_flg,
    CASE
    WHEN res.refrange LIKE '%-%'
    THEN TRIM(SPLIT(res.refrange, '-')[0])
    ELSE res.refrange
    END                                                                        AS lab_result_norm_min_msrmt,
    CASE
    WHEN res.refrange LIKE '%-%'
    THEN SUBSTRING(res.refrange, INSTR(res.refrange, '-') + 1)
    END                                                                        AS lab_result_norm_max_msrmt,
    res.resultstatus                                                           AS lab_result_stat_cd,
    CASE WHEN res.resultstatus IS NOT NULL THEN 'RESULT_STATUS' END            AS lab_result_stat_cd_qual,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    CASE WHEN rbf.resultid IS NOT NULL
        THEN rbf.recordeddttm ELSE res.recordeddttm END                        AS data_captr_dt,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN TRIM(COALESCE(res.auditdataflag, '')) = '0' THEN 'Current Record: '
            WHEN TRIM(COALESCE(res.auditdataflag, '')) = '1' THEN 'Historical Record: ' ELSE ''
            END,
            CASE
            WHEN TRIM(UPPER(res.errorflag)) = 'Y' THEN 'Entered in Error: ' ELSE ''
            END
            ), 2
        )                                                                      AS rec_stat_cd,
    'results'                                                                  AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_results res
    LEFT JOIN transactional_encounters enc ON res.gen2patientid = enc.gen2patientid
    AND res.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON res.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = res.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON res.genclientid = clt.genclientid
    LEFT JOIN results_backfill rbf ON res.genpatientid = rbf.genpatientid
    AND res.resultid = rbf.resultid
    AND res.versionid = rbf.versionid
WHERE res.gen2patientid IS NOT NULL
