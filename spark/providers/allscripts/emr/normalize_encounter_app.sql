SELECT
    CONCAT(
        '25_', app.gen2patientid, '_', app.appointmentid
        )                                                                      AS hv_enc_id,
    CASE WHEN app.input_file_name rlike '.*tier2.*' THEN '{batch_id}_201' ELSE '{batch_id}_01'
    END                                                                        AS data_set_nm,
    app.rectypeversion                                                         AS src_vrsn_id,
    app.genclientid                                                            AS vdr_org_id,
    app.primarykey                                                             AS vdr_enc_id,
    CASE
    WHEN app.primarykey IS NOT NULL THEN 'PRIMARYKEY'
    END                                                                        AS vdr_enc_id_qual,
    CONCAT('25_', app.gen2patientid, '_', app.encounterid)                     AS vdr_alt_enc_id,
    CASE
    WHEN app.gen2patientid IS NOT NULL AND app.encounterid IS NOT NULL
    THEN 'GEN2PATIENTID_ENCOUNTERID'
    END                                                                        AS vdr_alt_enc_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', app.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    EXTRACT_DATE(
        app.startdttm, '%Y-%m-%d', NULL,
        CASE
        WHEN TRIM(UPPER(app.status)) <> 'PENDING'
        THEN CAST({max_cap}                                                    AS DATE)
        END)                                                                   AS enc_start_dt,
    EXTRACT_DATE(
        app.enddttm, '%Y-%m-%d', NULL,
        CASE
        WHEN TRIM(UPPER(app.status)) <> 'PENDING'
        THEN CAST({max_cap}                                                    AS DATE)
        END)                                                                   AS enc_end_dt,
    'APPOINTMENT'                                                              AS enc_vst_typ_cd,
    app.gen2providerid                                                         AS enc_rndrg_prov_vdr_id,
    CASE
    WHEN app.gen2providerid IS NOT NULL
    THEN 'GEN2PROVIDERID'
    END                                                                        AS enc_rndrg_prov_vdr_id_qual,
    UPPER(prv.npi_txncode)                                                     AS enc_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS enc_rndrg_prov_alt_speclty_id,
    CASE
    WHEN prv.specialty IS NOT NULL
    THEN 'SPECIALTY'
    END                                                                        AS enc_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))                                             AS enc_rndrg_prov_state_cd,
    app.status                                                                 AS enc_stat_cd,
    CASE
    WHEN app.status IS NOT NULL
    THEN 'APPOINTMENT_STATUS'
    END                                                                        AS enc_stat_cd_qual,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    app.recordeddttm                                                           AS data_captr_dt,
    CASE
    WHEN COALESCE(app.auditdataflag, '') = '0'
    THEN 'Current Record'
    WHEN COALESCE(app.auditdataflag, '') = '1'
    THEN 'Historical Record'
    END                                                                        AS rec_stat_cd,
    'appointments'                                                             AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_appointments app
    LEFT JOIN transactional_encounters enc ON app.gen2patientid = enc.gen2patientid AND app.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON app.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = app.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON app.genclientid = clt.genclientid
WHERE app.gen2patientid IS NOT NULL
