SELECT
    CONCAT('25_', vac.gen2patientid, '_', vac.vaccineid, '_', vac.versionid)   AS hv_proc_id,
    vac.rectypeversion                                                         AS src_vrsn_id,
    vac.genclientid                                                            AS vdr_org_id,
    vac.primarykey                                                             AS vdr_proc_id,
    CASE WHEN vac.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_proc_id_qual,
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
    vac.administereddttm                                                       AS proc_dt,
    vac.gen2providerid                                                         AS proc_rndrg_prov_vdr_id,
    CASE WHEN vac.gen2providerid IS NOT NULL THEN  'GEN2PROVIDERID' END        AS proc_rndrg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS proc_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS proc_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                           AS proc_rndrg_prov_state_cd,
    vac.cvx                                                                    AS proc_cd,
    CASE WHEN vac.cvx IS NOT NULL THEN 'VACCINES.CVX' END                      AS proc_cd_qual,
    vac.ndc                                                                    AS proc_ndc,
    regexp_extract(vac.dose, '^([0-9]*\.[0-9]*)', 1)                           AS proc_unit_qty,
    regexp_extract(vac.dose, '([a-zA-z]*)$', 1)                                AS proc_uom,
    TRIM(UPPER(vac.status))                                                    AS proc_stat_cd,
    CASE WHEN vac.status IS NOT NULL THEN 'VACCINE_STATUS' END                 AS proc_stat_cd_qual,
    vac.routeofadmin                                                           AS proc_admin_rte_cd,
    vac.bodysite                                                               AS proc_admin_site_cd,
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
