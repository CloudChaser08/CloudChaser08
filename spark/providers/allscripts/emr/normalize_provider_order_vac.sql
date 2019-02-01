SELECT
    CONCAT('25_', vac.gen2patientid, '_', vac.vaccineid, '_', vac.versionid)   AS hv_prov_ord_id,
    vac.rectypeversion                                                         AS src_vrsn_id,
    vac.genclientid                                                            AS vdr_org_id,
    vac.primarykey                                                             AS vdr_prov_ord_id,
    CASE WHEN vac.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_prov_ord_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', vac.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    CONCAT('25_', vac.gen2patientid, '_', vac.encounterid)                     AS hv_enc_id,
    enc.encounterdttm                                                          AS enc_dt,
    vac.administereddttm                                                       AS prov_ord_dt,
    vac.gen2providerid                                                         AS ordg_prov_vdr_id,
    CASE WHEN vac.gen2providerid IS NOT NULL THEN  'GEN2PROVIDERID' END        AS ordg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS ordg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS ordg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS ordg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                           AS ordg_prov_state_cd,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN 0 <> LENGTH(TRIM(vac.routeofadmin))
            THEN CONCAT('Route: ', TRIM(vac.routeofadmin), ' | ')
            ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(vac.bodysite))
            THEN CONCAT('Site: ', TRIM(vac.bodysite), ' | ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(vac.dose))
            THEN CONCAT('Dose: ', TRIM(vac.dose), ' | ') ELSE ''
            END
            ), 3
        )                                                                      AS prov_ord_nm,
    vac.ndc                                                                    AS prov_ord_ndc,
    vac.cvx                                                                    AS prov_ord_vcx_cd,
    vac.name                                                                   AS prov_ord_vcx_nm,
    TRIM(UPPER(vac.status))                                                    AS prov_ord_stat_cd,
    CASE WHEN vac.status IS NOT NULL THEN 'VACCINE_STATUS' END                 AS prov_ord_stat_cd_qual,
    vac.administereddttm                                                       AS prov_ord_complt_dt,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    vac.recordeddttm                                                           AS data_captr_dt,
    CASE
    WHEN TRIM(COALESCE(enc.auditdataflag, '')) = '0' THEN 'Current Record'
    WHEN TRIM(COALESCE(enc.auditdataflag, '')) = '1' THEN 'Historical Record'
    END                                                                        AS rec_stat_cd,
    'vaccines'                                                                 AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_vaccines vac
    LEFT JOIN transactional_encounters enc ON vac.gen2patientid = enc.gen2patientid
    AND vac.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON vac.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = vac.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON vac.genclientid = clt.genclientid
WHERE vac.gen2patientID IS NOT NULL
