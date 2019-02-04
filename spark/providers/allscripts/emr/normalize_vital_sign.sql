SELECT
    CONCAT('25_', vit.gen2patientid, '_', vit.vitalid, '_', vit.versionid)     AS hv_vit_sign_id,
    vit.rectypeversion                                                         AS src_vrsn_id,
    vit.genclientid                                                            AS vdr_org_id,
    vit.primarykey                                                             AS vdr_vit_sign_id,
    CASE WHEN vit.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                 AS vdr_vit_sign_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', vit.gen2patientid))                       AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                     AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                        AS ptnt_gender_cd,
    ptn.state                                                                  AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                 AS ptnt_zip3_cd,
    CONCAT('25_', vit.gen2patientid, '_', vit.encounterid)                     AS hv_enc_id,
    enc.encounterdttm                                                          AS enc_dt,
    vit.performeddttm                                                          AS vit_sign_dt,
    vit.gen2providerid                                                         AS vit_sign_rndrg_prov_vdr_id,
    CASE WHEN vit.gen2providerid IS NOT NULL THEN 'GEN2PROVIDERID' END         AS vit_sign_rndrg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                               AS vit_sign_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                       AS vit_sign_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                   AS vit_sign_rndrg_prov_alt_speclty_id_qual,
    prv.state                                                                  AS vit_sign_rndrg_prov_state_cd,
    ref.gen_ref_1_txt                                                          AS vit_sign_typ_cd,
    CASE WHEN ref.gen_ref_1_txt IS NOT NULL THEN 'VITAL_NAME' END              AS vit_sign_typ_cd_qual,
    CASE WHEN ref.gen_ref_1_txt IS NOT NULL
    THEN CONVERT_VALUE(vit.value, ref.gen_ref_itm_desc)
    ELSE vit.value
    END                                                                        AS vit_sign_msrmt,
    ref.gen_ref_2_txt                                                          AS vit_sign_uom,
    TRIM(UPPER(vit.status))                                                    AS vit_sign_stat_cd,
    CASE WHEN vit.status IS NOT NULL THEN 'VITAL_STATUS' END                   AS vit_sign_stat_cd_qual,
    UPPER(clt.sourcesystemcode)                                                AS data_src_cd,
    vit.recordeddttm                                                           AS data_captr_dt,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN TRIM(COALESCE(vit.auditdataflag, '')) = '0' THEN 'Current Record: '
            WHEN TRIM(COALESCE(vit.auditdataflag, '')) = '1' THEN 'Historical Record: ' ELSE ''
            END,
            CASE
            WHEN TRIM(UPPER(vit.errorflag)) = 'Y' THEN 'Entered in Error: ' ELSE ''
            END
            ), 2
        )                                                                      AS rec_stat_cd,
    'vitals'                                                                   AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                      AS allscripts_date_partition
FROM transactional_vitals vit
    LEFT JOIN transactional_encounters enc ON vit.gen2patientid = enc.gen2patientid AND vit.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON vit.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = vit.hv_gen2providerid
    LEFT JOIN transactional_clients clt ON vit.genclientid = clt.genclientid
    LEFT JOIN ref_gen_ref ref ON ref.whtlst_flg = 'Y'
    AND ref.gen_ref_domn_nm = 'allscripts_emr.vitals'
    AND TRIM(UPPER(vit.name)) = ref.gen_ref_cd
    AND TRIM(UPPER(COALESCE(vit.units, ''))) = COALESCE(ref.gen_ref_itm_nm, '')
WHERE vit.gen2patientid IS NOT NULL
    AND ref.gen_ref_cd IS NOT NULL
