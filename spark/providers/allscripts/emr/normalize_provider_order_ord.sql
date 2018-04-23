SELECT
    CONCAT('25_', ord.gen2patientid, '_', ord.orderid, '_', ord.versionid)  AS hv_prov_ord_id,
    ord.rectypeversion                                                      AS src_vrsn_id,
    ord.genclientid                                                         AS vdr_org_id,
    ord.primarykey                                                          AS vdr_prov_ord_id,
    CASE WHEN ord.primarykey IS NOT NULL THEN 'PRIMARYKEY' END              AS vdr_prov_ord_id_qual,
    pay.hvid                                                                AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                  AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                     AS ptnt_gender_cd,
    ptn.state                                                               AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)              AS ptnt_zip3_cd,
    CONCAT('25_', ord.gen2patientid, '_', ord.encounterid)                  AS hv_enc_id,
    enc.encounterdttm                                                       AS enc_dt,
    ord.orderdttm                                                           AS prov_ord_dt,
    CASE
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, '')))
    THEN TRIM(ord.performinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, '')))
    THEN TRIM(ord.orderinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, '')))
    THEN TRIM(ord.approvinggen2providerid)
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, '')))
    THEN TRIM(ord.gen2providerid)
    END                                                                     AS ordg_prov_vdr_id,
    CASE
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, ''))) THEN 'PERFORMINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, ''))) THEN 'ORDERINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, ''))) THEN 'APPROVINGGEN2PROVIDERID'
    WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, ''))) THEN 'GEN2PROVIDERID'
    END                                                                     AS ordg_prov_vdr_id_qual,
    TRIM(UPPER(prv.npi_txncode))                                            AS ordg_prov_nucc_taxnmy_cd,
    UPPER(prv.specialty)                                                    AS ordg_prov_alt_speclty_id,
    CASE WHEN prv.specialty IS NOT NULL THEN 'SPECIALTY' END                AS ordg_prov_alt_speclty_id_qual,
    UPPER(prv.state)                                                        AS ordg_prov_state_cd,
    ord.type                                                                AS prov_ord_ctgy_cd,
    CASE WHEN ord.type IS NOT NULL THEN 'ORDER_TYPE' END                    AS prov_ord_ctgy_cd_qual,
    ord.source                                                              AS prov_ord_typ_cd,
    CASE WHEN ord.source IS NOT NULL THEN 'SPECIMEN_SOURCE' END             AS prov_ord_typ_cd_qual,
    ord.name                                                                AS prov_ord_nm,
    NULL                                                                    AS prov_ord_ndc,
    NULL                                                                    AS prov_ord_vcx_cd,
    NULL                                                                    AS prov_ord_vcx_nm,
    ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n]                  AS prov_ord_diag_cd,
    CASE
    WHEN ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n] IS NULL THEN NULL
    WHEN n2.n = 0 THEN '01' ELSE '02' END                              AS prov_ord_diag_cd_qual,
    TRIM(UPPER(ord.status))                                                 AS prov_ord_stat_cd,
    CASE WHEN ord.status IS NOT NULL THEN 'ORDER_STATUS' END                AS prov_ord_stat_cd_qual,
    NULL                                                                    AS prov_ord_complt_dt,
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
        CASE WHEN 0 <> LENGTH(TRIM(COALESCE(ord.performinggen2providerid, '')))
        THEN ord.performinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.orderinggen2providerid, '')))
        THEN ord.orderinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.approvinggen2providerid, '')))
        THEN ord.approvinggen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ord.gen2providerid, '')))
        THEN ord.gen2providerid
        ELSE ord.hv_gen2providerid
        END
        )
    LEFT JOIN transactional_clients clt ON ord.genclientid = clt.genclientid
    CROSS JOIN diag_exploder n2
WHERE TRIM(UPPER(COALESCE(ord.type, ''))) <> 'LABORATORY'
    AND ord.gen2patientid IS NOT NULL
    AND 0 = LENGTH(TRIM(COALESCE(ord.cpt4, '')))
    AND 0 = LENGTH(TRIM(COALESCE(ord.hcpcs, '')))
    AND (
        ARRAY(ord.billingicd9code, ord.billingicd10code)[n2.n] IS NOT NULL
        OR (
            COALESCE(ord.billingicd9code, ord.billingicd10code) IS NULL
            AND n2.n = 0
            )
        )
