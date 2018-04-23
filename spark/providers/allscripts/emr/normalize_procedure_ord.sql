SELECT
    CONCAT('25_', ord.gen2patientid, '_', ord.orderid, '_', ord.versionid)  AS hv_proc_id,
    ord.rectypeversion                                                      AS src_vrsn_id,
    ord.genclientid                                                         AS vdr_org_id,
    ord.primarykey                                                          AS vdr_proc_id,
    CASE WHEN ord.primarykey IS NOT NULL THEN 'PRIMARYKEY' END              AS vdr_proc_id_qual,
    pay.hvid                                                                AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                  AS ptnt_birth_yr,
    CASE
    WHEN UPPER(COALESCE(ptn.gender, pay.gender, 'U')) IN ('F', 'M', 'U')
    THEN UPPER(COALESCE(ptn.gender, pay.gender, 'U')) ELSE 'U'
    END                                                                     AS ptnt_gender_cd,
    ptn.state                                                               AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)              AS ptnt_zip3_cd,
    CONCAT('25_', ord.gen2patientid, '_', ord.encounterid)                  AS hv_enc_id,
    enc.encounterdttm                                                       AS enc_dt,
    ord.orderdttm                                                           AS proc_dt,
    CASE
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, ''))) THEN TRIM(ord.performinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, ''))) THEN TRIM(ord.orderinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, ''))) THEN TRIM(ord.approvinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, ''))) THEN TRIM(ord.gen2providerid)
    END                                                                     AS proc_rndrg_prov_vdr_id,
    CASE
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, ''))) THEN 'PERFORMINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, ''))) THEN 'ORDERINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, ''))) THEN 'APPROVINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, ''))) THEN 'GEN2PROVIDERID'
    END                                                                     AS proc_rndrg_prov_vdr_id_qual,
    UPPER(prv.npi_txncode)                                                  AS proc_rndrg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                    AS proc_rndrg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                AS proc_rndrg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                        AS proc_rndrg_prov_state_cd,
    ARRAY(ord.cpt4, ord.hcpcs)[n.n]                                         AS proc_cd,
    CASE
    WHEN ARRAY(ord.cpt4, ord.hcpcs)[n.n] IS NULL THEN NULL
    WHEN n.n = 0
    THEN 'CPTCODE'
    ELSE 'HCPCS'
    END                                                                     AS proc_cd_qual,
    ord.cptmod                                                              AS proc_cd_1_modfr,
    SUBSTRING(
        CONCAT(
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.type, '')))
            THEN CONCAT(ord.type, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.name, '')))
            THEN CONCAT(ord.name, ': ')
            ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.source, '')))
            THEN CONCAT(ord.source, ': ')
            ELSE ''
            END
            ), 1, LENGTH(CONCAT(
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.type, '')))
                THEN CONCAT(ord.type, ': ') ELSE ''
                END,
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.name, '')))
                THEN CONCAT(ord.name, ': ')
                ELSE ''
                END,
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.source, '')))
                THEN CONCAT(ord.source, ': ')
                ELSE ''
                END
                )) - 2
        )                                                                   AS proc_alt_cd,
    CASE WHEN SUBSTRING(
        CONCAT(
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.type, '')))
            THEN CONCAT(ord.type, ': ') ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.name, '')))
            THEN CONCAT(ord.name, ': ')
            ELSE ''
            END,
            CASE
            WHEN 0 <> LENGTH(TRIM(COALESCE(ord.source, '')))
            THEN CONCAT(ord.source, ': ')
            ELSE ''
            END
            ), 1, LENGTH(CONCAT(
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.type, '')))
                THEN CONCAT(ord.type, ': ') ELSE ''
                END,
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.name, '')))
                THEN CONCAT(ord.name, ': ')
                ELSE ''
                END,
                CASE
                WHEN 0 <> LENGTH(TRIM(COALESCE(ord.source, '')))
                THEN CONCAT(ord.source, ': ')
                ELSE ''
                END
                )) - 2
        ) IS NOT NULL
    THEN 'STATUS_TYPE_CATEGORY_LEVEL1_LEVEL2_LEVEL3' END                    AS proc_alt_cd_qual,
    ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n]                  AS proc_diag_cd,
    CASE
    WHEN ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n] IS NULL THEN NULL
    WHEN n2.n = 0 THEN '01' ELSE '02' END                                   AS proc_diag_cd_qual,
    TRIM(UPPER(ord.status))                                                 AS proc_stat_cd,
    CASE WHEN ord.status IS NOT NULL THEN 'ORDER_STATUS' END                AS proc_stat_cd_qual,
    UPPER(clt.sourcesystemcode)                                             AS data_src_cd,
    ord.recordeddttm                                                        AS data_captr_dt,
    CASE
    WHEN TRIM(COALESCE(ord.auditdataflag, '')) = '0'
    THEN 'Current Record'
    WHEN TRIM(COALESCE(ord.auditdataflag, '')) = '1'
    THEN 'Historical Record'
    END                                                                     AS rec_stat_cd,
    'orders'                                                                AS prmy_src_tbl_nm,
    enc.encounterdttm                                                       AS allscripts_date_partition
FROM transactional_orders ord
    LEFT JOIN transactional_encounters enc ON ord.gen2patientid = enc.gen2patientid
    AND ord.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON ord.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv ON prv.gen2providerid = (
        CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, ''))) THEN ord.performinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, ''))) THEN ord.orderinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, ''))) THEN ord.approvinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, ''))) THEN ord.gen2providerid
        ELSE ord.hv_gen2providerid
        END
        )
    LEFT JOIN transactional_clients clt ON ord.genclientid = clt.genclientid
    CROSS JOIN proc_exploder n
    CROSS JOIN diag_exploder n2
WHERE UPPER(COALESCE(ord.type, '')) <> 'LABORATORY'
        AND (0 <> LENGTH(COALESCE(ord.cpt4, '')) OR 0 <> LENGTH(COALESCE(ord.hcpcs, '')))
        AND (
            ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n] IS NOT NULL
            OR (
                COALESCE(ord.billingicd9code, ord.billingicd10code) IS NULL
                AND n2.n = 0
                )
            )
