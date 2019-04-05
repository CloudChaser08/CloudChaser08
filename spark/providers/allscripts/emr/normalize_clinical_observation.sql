SELECT
    CONCAT('25_', alg.gen2patientid, '_', alg.allergyid, '_', alg.versionid)   AS hv_clin_obsn_id,
    alg.rectypeversion                                                         AS src_vrsn_id,
    alg.genclientid                                                            AS vdr_org_id,
    alg.primarykey                                                             AS vdr_clin_obsn_id,
    CASE WHEN alg.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_clin_obsn_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', alg.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    CONCAT('25_', alg.gen2patientid, '_', alg.encounterid)                     AS hv_enc_id,
    enc.encounterdttm                                                          AS enc_dt,
    enc.encounterdttm                                                          AS clin_obsn_dt,
    alg.gen2providerid                                                         AS clin_obsn_rndrg_prov_vdr_id,
    CASE WHEN alg.gen2providerid IS NOT NULL THEN 'GEN2PROVIDERID' END         AS clin_obsn_rndrg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS clin_obsn_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS clin_obsn_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS clin_obsn_rndrg_prov_alt_speclty_id_qual,
    prv.state                                                                  AS clin_obsn_rndrg_prov_state_cd,
    alg.reactionDTTM                                                           AS clin_obsn_onset_dt,
    CASE
    WHEN SUBSTRING(TRIM(UPPER(alg.type)), 1, 7) <> 'ALLERGY'
    THEN CONCAT('ALLERGY TO ', TRIM(UPPER(alg.type)))
    ELSE TRIM(UPPER(alg.type))
    END clin_obsn_typ_cd,
    CASE WHEN (
        CASE
        WHEN SUBSTRING(TRIM(UPPER(alg.type)), 1, 7) <> 'ALLERGY'
        THEN CONCAT('ALLERGY TO ', TRIM(UPPER(alg.type)))
        ELSE TRIM(UPPER(alg.type))
        END
        ) IS NOT NULL THEN 'ALLERGY_TYPE' END                                  AS clin_obsn_typ_cd_qual,
    ARRAY(
        CASE WHEN alg.rxnorm != '0' THEN alg.rxnorm END,
        CASE WHEN alg.gpi != '0' THEN alg.gpi END,
        CASE WHEN alg.ddi != '0' THEN alg.ddi END
        )[n.n]                                                                 AS clin_obsn_substc_cd,
    CASE
    WHEN ARRAY(
        CASE WHEN alg.rxnorm != '0' THEN alg.rxnorm END,
        CASE WHEN alg.gpi != '0' THEN alg.gpi END,
        CASE WHEN alg.ddi != '0' THEN alg.ddi END
        )[n.n] IS NOT NULL THEN ARRAY('RXNORM', 'GPI', 'DDI')[n.n]
    END                                                                        AS clin_obsn_substc_cd_qual,
    TRIM(UPPER(alg.name))                                                      AS clin_obsn_cd,
    CASE WHEN alg.name IS NOT NULL THEN 'ALLERGY_NAME' END                     AS clin_obsn_cd_qual,
    alg.ndc                                                                    AS clin_obsn_ndc,
    alg.snomed                                                                 AS clin_obsn_snomed_cd,
    TRIM(UPPER(alg.reactions))                                                 AS clin_obsn_result_cd,
    CASE WHEN alg.reactions IS NOT NULL THEN 'ALLERGY_REACTIONS' END           AS clin_obsn_result_cd_qual,
    TRIM(UPPER(alg.status))                                                    AS clin_obsn_stat_cd,
    CASE WHEN alg.status IS NOT NULL THEN 'ALLERGY_STATUS' END                 AS clin_obsn_stat_cd_qual,
    CASE
    WHEN TRIM(UPPER(alg.unverifiedflag)) = 'Y' THEN 'N'
    WHEN TRIM(UPPER(alg.unverifiedflag)) = 'N' THEN 'Y'
    END                                                                        AS clin_obsn_verfd_by_prov_flg,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    alg.recordeddttm                                                           AS data_captr_dt,
    CASE
    WHEN TRIM(COALESCE(alg.auditdataflag, '')) = '0'
    THEN 'Current Record'
    WHEN TRIM(COALESCE(alg.auditdataflag, '')) = '1'
    THEN 'Historical Record'
    END                                                                        AS rec_stat_cd,
    'allergies'                                                                AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_allergies alg
    LEFT JOIN transactional_encounters enc ON alg.gen2patientid = enc.gen2patientid
    AND alg.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON alg.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = alg.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON alg.genclientid = clt.genclientid
    CROSS JOIN clin_obsn_exploder n
WHERE alg.gen2patientid IS NOT NULL AND (
        ARRAY(
            CASE WHEN alg.rxnorm != '0' THEN alg.rxnorm END,
            CASE WHEN alg.gpi != '0' THEN alg.gpi END,
            CASE WHEN alg.ddi != '0' THEN alg.ddi END
            )[n.n] IS NOT NULL
        OR (
            COALESCE(
                CASE WHEN alg.rxnorm != '0' THEN alg.rxnorm END,
                CASE WHEN alg.gpi != '0' THEN alg.gpi END,
                CASE WHEN alg.ddi != '0' THEN alg.ddi END
                ) IS NULL AND n.n = 0
            )
        )
