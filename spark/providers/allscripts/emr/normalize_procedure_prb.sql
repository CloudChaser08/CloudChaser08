SELECT
    CONCAT('25_', prb.gen2patientid, '_', prb.problemid, '_', prb.versionid)   AS hv_proc_id,
    CASE WHEN prb.input_file_name rlike '.*tier2.*' THEN '{batch_id}_201' ELSE '{batch_id}_01'
    END                                                                        AS data_set_nm,
    prb.rectypeversion                                                         AS src_vrsn_id,
    prb.genclientid                                                            AS vdr_org_id,
    prb.primarykey                                                             AS vdr_proc_id,
    CASE WHEN prb.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_proc_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', prb.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    CONCAT('25_', prb.gen2patientid, '_', prb.encounterid)                     AS hv_enc_id,
    enc.encounterDTTM                                                          AS enc_dt,
    prb.diagnosisdttm                                                          AS proc_dt,
    prb.gen2providerid                                                         AS proc_rndrg_prov_vdr_id,
    CASE
    WHEN prb.gen2providerid IS NOT NULL
    THEN 'GEN2PROVIDERID'
    END                                                                        AS proc_rndrg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS proc_rndrg_prov_nucc_taxnmy_cd,
    TRIM(UPPER(prv.specialty))                                                 AS proc_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                           AS proc_rndrg_prov_state_cd,
    prb.cptcode                                                                AS proc_cd,
    CASE WHEN prb.cptcode IS NOT NULL THEN 'CPTCODE' END                       AS proc_cd_qual,
    UPPER(prb.snomed)                                                          AS proc_snomed_cd,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.type, '')))
            THEN CONCAT(prb.type, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.category, '')))
            THEN CONCAT(prb.category, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level1, '')))
            THEN CONCAT(prb.level1, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level2, '')))
            THEN CONCAT(prb.level2, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level3, '')))
            THEN CONCAT(prb.level3, ': ') ELSE ''
            END
            ), 2
        )                                                                      AS proc_alt_cd,
    CASE WHEN REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.type, '')))
            THEN CONCAT(prb.type, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.category, '')))
            THEN CONCAT(prb.category, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level1, '')))
            THEN CONCAT(prb.level1, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level2, '')))
            THEN CONCAT(prb.level2, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(prb.level3, '')))
            THEN CONCAT(prb.level3, ': ') ELSE ''
            END
            ), 2
        ) IS NOT NULL THEN 'TYPE_CATEGORY_LEVEL1_LEVEL2_LEVEL3'
    END                                                                        AS proc_alt_cd_qual,
    ARRAY(prb.icd9, prb.icd10)[n.n]                                            AS proc_diag_cd,
    CASE
    WHEN ARRAY(prb.icd9, prb.icd10)[n.n] IS NULL THEN NULL
    WHEN n.n = 0 THEN '01' ELSE '02'
    END                                                                        AS proc_diag_cd_qual,
    TRIM(prb.status)                                                           AS proc_stat_cd,
    CASE
    WHEN prb.status IS NOT NULL
    THEN 'PROBLEM_STATUS'
    END                                                                        AS proc_stat_cd_qual,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    prb.recordeddttm                                                           AS data_captr_dt,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN TRIM(COALESCE(prb.auditdataflag, '')) = '0'
            THEN 'Current Record: '
            WHEN TRIM(COALESCE(prb.auditdataflag, '')) = '1'
            THEN 'Historical Record: ' ELSE ''
            END,
            CASE
            WHEN TRIM(UPPER(prb.errorflag)) = 'Y'
            THEN 'Entered in Error: '
            ELSE ''
            END
            ), 2
        )                                                                      AS rec_stat_cd,
    'problems'                                                                 AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_problems prb
    LEFT JOIN transactional_encounters enc ON prb.gen2patientid = enc.gen2patientid
    AND prb.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON prb.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = prb.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON prb.genclientid = clt.genclientid
    CROSS JOIN diag_exploder n
WHERE prb.gen2patientid IS NOT NULL
    AND (
        ARRAY(prb.icd9, prb.icd10)[n.n] IS NOT NULL OR (COALESCE(prb.icd9, prb.icd10) IS NULL AND n.n = 0)
        )
