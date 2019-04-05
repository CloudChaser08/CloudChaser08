SELECT
    CONCAT('25_', enc.gen2patientid, '_', enc.encounterid)                     AS hv_enc_id,
    enc.rectypeversion                                                         AS src_vrsn_id,
    enc.genclientid                                                            AS vdr_org_id,
    enc.primarykey                                                             AS vdr_enc_id,
    CASE WHEN enc.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_enc_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', enc.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    EXTRACT_DATE(
        enc.encounterdttm, '%Y-%m-%d', NULL, CAST({max_cap}                    AS DATE)
        )                                                                      AS enc_start_dt,
    COALESCE(enc.renderinggen2providerID, enc.gen2providerID)                  AS enc_rndrg_prov_vdr_id,
    CASE
    WHEN COALESCE(enc.renderinggen2providerID, enc.gen2providerID) IS NOT NULL
    THEN 'GEN2PROVIDERID'
    END                                                                        AS enc_rndrg_prov_vdr_id_qual,
    UPPER(prv.npi_txncode)                                                     AS enc_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS enc_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS enc_rndrg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                           AS enc_rndrg_prov_state_cd,
    UPPER(enc.type)                                                            AS enc_typ_cd,
    CASE WHEN enc.type IS NOT NULL THEN 'ENCOUNTER_TYPE' END                   AS enc_typ_cd_qual,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    enc.recordeddttm                                                           AS data_captr_dt,
    CASE WHEN enc.auditdataflag = '0' THEN 'Current Record'
    WHEN enc.auditdataflag = '1' THEN 'Historical Record'
    END                                                                        AS rec_stat_cd,
    'encounters'                                                               AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_encounters enc
    LEFT JOIN transactional_patientdemographics ptn ON enc.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = (
        CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(enc.renderinggen2providerid, ''))) THEN enc.renderinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(enc.gen2providerid, ''))) THEN enc.gen2providerid
        ELSE enc.hv_gen2providerid
        END
        )
    LEFT JOIN transactional_clients clt ON enc.genclientid = clt.genclientid
WHERE enc.gen2patientid IS NOT NULL
