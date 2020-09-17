SELECT
    CONCAT('25_', prb.gen2patientid, '_', prb.problemid, '_', prb.versionid)   AS hv_diag_id,
    CASE WHEN prb.input_file_name rlike '.*tier2.*' THEN '{batch_id}_201' ELSE '{batch_id}_01'
    END                                                                        AS data_set_nm,
    prb.rectypeversion                                                         AS src_vrsn_id,
    prb.genclientid                                                            AS vdr_org_id,
    prb.primarykey                                                             AS vdr_diag_id,
    CASE
    WHEN prb.primarykey IS NOT NULL THEN 'PRIMARYKEY'
    END                                                                        AS vdr_diag_id_qual,
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
    prb.diagnosisdttm                                                          AS diag_dt,
    prb.gen2providerid                                                         AS diag_rndrg_prov_vdr_id,
    CASE
    WHEN prb.gen2providerid IS NOT NULL
    THEN 'GEN2PROVIDERID'
    END                                                                        AS diag_rndrg_prov_vdr_id_qual,
    UPPER(prv.npi_txncode)                                                     AS diag_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS diag_rndrg_prov_alt_speclty_id,
    CASE
    WHEN prv.specialty IS NOT NULL
    THEN 'SPECIALTY'
    END                                                                        AS diag_rndrg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                           AS diag_rndrg_prov_state_cd,
    prb.onsetdttm                                                              AS diag_onset_dt,
    prb.resolveddttm                                                           AS diag_resltn_dt,
    ARRAY(prb.icd9, prb.icd10)[n.n]                                            AS diag_cd,
    CASE
    WHEN ARRAY(prb.icd9, prb.icd10)[n.n] IS NULL THEN NULL
    WHEN n.n = 0 THEN '01' ELSE '02'
    END                                                                        AS diag_cd_qual,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN prb.level1 IS NOT NULL
            THEN CONCAT(prb.level1, ': ')
            ELSE ''
            END,
            CASE
            WHEN prb.level2 IS NOT NULL
            THEN CONCAT(prb.level2, ': ')
            ELSE ''
            END,
            CASE
            WHEN prb.level3 IS NOT NULL
            THEN CONCAT(prb.level3, ': ')
            ELSE ''
            END
            ), 2
        )                                                                      AS diag_alt_cd,
    CASE
    WHEN COALESCE(TRIM(REMOVE_LAST_CHARS(
                CONCAT(
                    CASE
                    WHEN prb.level1 IS NOT NULL
                    THEN CONCAT(prb.level1, ': ')
                    ELSE ''
                    END,
                    CASE
                    WHEN prb.level2 IS NOT NULL
                    THEN CONCAT(prb.level2, ': ')
                    ELSE ''
                    END,
                    CASE
                    WHEN prb.level3 IS NOT NULL
                    THEN CONCAT(prb.level3, ': ')
                    ELSE ''
                    END
                    ), 2
                )), '') <> '' THEN 'LEVEL1_LEVEL2_LEVEL3'
    END                                                                        AS diag_alt_cd_qual,
    prb.name                                                                   AS diag_nm,
    TRIM(prb.status)                                                           AS diag_stat_cd,
    CASE
    WHEN prb.status IS NOT NULL
    THEN 'PROBLEM_STATUS'
    END                                                                        AS diag_stat_cd_qual,
    UPPER(prb.type)                                                            AS diag_stat_nm,
    UPPER(prb.category)                                                        AS diag_stat_desc,
    UPPER(prb.snomed)                                                          AS diag_snomed_cd,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    prb.recordeddttm                                                           AS data_captr_dt,
    prb.auditdataflag                                                          AS rec_stat_cd,
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
WHERE prb.gen2patientid IS NOT NULL AND (
        ARRAY(prb.icd9, prb.icd10)[n.n] IS NOT NULL OR (
            COALESCE(prb.icd9, prb.icd10) IS NULL AND n.n = 0
            )
        )
