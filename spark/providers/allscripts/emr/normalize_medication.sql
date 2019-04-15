SELECT
    CONCAT('25_', med.gen2patientid, '_', med.medid, '_', med.versionid)                         AS hv_medctn_id,
    CASE WHEN med.input_file_name rlike '.*tier2.*' THEN '{batch_id}_201' ELSE '{batch_id}_01'
    END                                                                                          AS data_set_nm,
    med.rectypeversion                                                                           AS src_vrsn_id,
    med.genclientid                                                                              AS vdr_org_id,
    med.primarykey                                                                               AS vdr_medctn_ord_id,
    CASE WHEN med.primarykey IS NOT NULL THEN 'PRIMARYKEY' END                                   AS vdr_medctn_ord_id_qual,
    COALESCE(pay.hvid, CONCAT('35_', med.gen2patientid))                                         AS hvid,
    COALESCE(ptn.dobyear, pay.yearofbirth)                                                       AS ptnt_birth_yr,
    CASE
    WHEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) IN ('F', 'M', 'U')
    THEN UPPER(SUBSTRING(COALESCE(ptn.gender, pay.gender, 'U'), 1, 1)) ELSE 'U'
    END                                                                                          AS ptnt_gender_cd,
    ptn.state                                                                                    AS ptnt_state_cd,
    SUBSTRING(COALESCE(ptn.zip3, pay.threedigitzip, ''), 1, 3)                                   AS ptnt_zip3_cd,
    CONCAT('25_', med.gen2patientid, '_', med.encounterid)                                       AS hv_enc_id,
    enc.encounterdttm                                                                            AS enc_dt,
    med.performeddate                                                                            AS medctn_admin_dt,
    med.prescribedbygen2providerid                                                               AS medctn_ordg_prov_vdr_id,
    CASE
    WHEN med.prescribedbygen2providerid IS NOT NULL THEN 'PRESCRIBEDBYGEN2PROVIDERID'
    END                                                                                          AS medctn_ordg_prov_vdr_id_qual,
    TRIM(UPPER(prv1.npi_txncode))                                                                AS medctn_ordg_prov_nucc_taxnmy_cd,
    UPPER(prv1.specialty)                                                                        AS medctn_ordg_prov_alt_speclty_id,
    CASE WHEN prv1.specialty IS NOT NULL THEN 'SPECIALTY' END                                    AS medctn_ordg_prov_alt_speclty_id_qual,
    prv1.state                                                                                   AS medctn_ordg_prov_state_cd,
    CASE
    WHEN 0 <> LENGTH(TRIM(med.administeredbygen2providerID)) THEN TRIM(med.administeredbygen2providerID)
    WHEN 0 <> LENGTH(TRIM(med.gen2providerID)) THEN TRIM(med.gen2providerID)
    END                                                                                          AS medctn_adminrg_fclty_vdr_id,
    TRIM(UPPER(prv2.npi_txncode))                                                                AS medctn_adminrg_fclty_nucc_taxnmy_cd,
    UPPER(prv2.specialty)                                                                        AS medctn_adminrg_fclty_alt_speclty_id,
    CASE WHEN prv2.specialty IS NOT NULL THEN 'SPECIALTY' END                                    AS medctn_adminrg_fclty_alt_speclty_id_qual,
    prv2.state                                                                                   AS medctn_adminrg_fclty_state_cd,
    med.startdttm                                                                                AS medctn_start_dt,
    med.enddttm                                                                                  AS medctn_end_dt,
    med.daystotake                                                                               AS medctn_ord_durtn_day_cnt,
    CASE
    WHEN 0 <> LENGTH(TRIM(med.prescribeaction)) AND 0 <> LENGTH(TRIM(med.status))
    THEN CONCAT(TRIM(med.prescribeaction), ' | ', TRIM(med.status))
    WHEN 0 <> LENGTH(TRIM(med.prescribeaction))
    THEN TRIM(med.prescribeaction)
    WHEN 0 <> LENGTH(TRIM(med.status))
    THEN TRIM(med.status)
    END                                                                                          AS medctn_ord_stat_cd,
    CASE WHEN (
        CASE
        WHEN 0 <> LENGTH(TRIM(med.prescribeaction)) AND 0 <> LENGTH(TRIM(med.status))
        THEN CONCAT(TRIM(med.prescribeaction), ' | ', TRIM(med.status))
        WHEN 0 <> LENGTH(TRIM(med.prescribeaction))
        THEN TRIM(med.prescribeaction)
        WHEN 0 <> LENGTH(TRIM(med.status))
        THEN TRIM(med.status)
        END
        ) IS NOT NULL THEN 'PRESCRIBEACTION_STATUS' END                                          AS medctn_ord_stat_cd_qual,
    med.ndc                                                                                      AS medctn_ndc,
    ARRAY(
        CASE WHEN med.cvx != '0' THEN med.cvx END,
        CASE WHEN med.rxnorm != '0' THEN med.rxnorm END,
        CASE WHEN med.gpi != '0' THEN med.gpi END,
        CASE WHEN med.ddi != '0' THEN med.ddi END
        )[n.n]                                                                                   AS medctn_alt_substc_cd,
    CASE
    WHEN ARRAY(
        CASE WHEN med.cvx != '0' THEN med.cvx END,
        CASE WHEN med.rxnorm != '0' THEN med.rxnorm END,
        CASE WHEN med.gpi != '0' THEN med.gpi END,
        CASE WHEN med.ddi != '0' THEN med.ddi END
        )[n.n] IS NOT NULL THEN ARRAY('CVX', 'RXNORM', 'GPI', 'DDI')[n.n]
    END                                                                                          AS medctn_alt_substc_cd_qual,
    CASE
    WHEN TRIM(UPPER(med.dawflag)) = 'Y' THEN 'N'
    WHEN TRIM(UPPER(med.dawflag)) = 'N' THEN 'Y'
    END                                                                                          AS medctn_genc_ok_flg,
    med.name                                                                                     AS medctn_genc_nm,
    med.quantitytodispense                                                                       AS medctn_dispd_qty,
    med.daysupply                                                                                AS medctn_days_supply_qty,
    med.frequencyperday                                                                          AS medctn_admin_freq_qty,
    med.frequency                                                                                AS medctn_admin_sched_cd,
    med.sig                                                                                      AS medctn_admin_sig_cd,
    med.form                                                                                     AS medctn_admin_form_nm,
    TRIM(CONCAT(med.strength, ' ', med.units))                                                   AS medctn_strth_txt,
    med.dose                                                                                     AS medctn_dose_txt,
    med.routeofadmin                                                                             AS medctn_admin_rte_txt,
    med.refills                                                                                  AS medctn_remng_rfll_qty,
    CASE WHEN TRIM(UPPER(med.sampleflag)) IN ('N', 'Y') THEN med.sampleflag END                  AS medctn_smpl_flg,
    CASE WHEN TRIM(UPPER(med.erxtransmittedflag)) IN ('N', 'Y') THEN med.erxtransmittedflag END  AS medctn_elect_rx_flg,
    UPPER(clt.sourcesystemcode)                                                                  AS data_src_cd,
    med.recordeddttm                                                                             AS data_captr_dt,
    REMOVE_LAST_CHARS(
        CONCAT(
            CASE
            WHEN TRIM(COALESCE(med.auditdataflag, '')) = '0' THEN 'Current Record: '
            WHEN TRIM(COALESCE(med.auditdataflag, '')) = '1' THEN 'Historical Record: ' ELSE ''
            END,
            CASE
            WHEN TRIM(UPPER(med.errorflag)) = 'Y' THEN 'Entered in Error: ' ELSE ''
            END
            ), 2
        )                                                                                        AS rec_stat_cd,
    'medications'                                                                                AS prmy_src_tbl_nm,
    EXTRACT_DATE(
        SUBSTRING(enc.encounterdttm, 1, 10), '%Y-%m-%d', NULL, CAST({max_cap} AS DATE)
        )                                                                                        AS allscripts_date_partition
FROM transactional_medications med
    LEFT JOIN transactional_encounters enc ON med.gen2patientid = enc.gen2patientid
    AND med.encounterid = enc.encounterid
    LEFT JOIN transactional_patientdemographics ptn ON med.gen2patientid = ptn.gen2patientid
    LEFT JOIN matching_payload pay ON UPPER(ptn.gen2patientID) = UPPER(pay.personid)
    LEFT JOIN transactional_providers prv1 ON prv1.gen2providerid = med.hv_prescribedbygen2providerid
    LEFT JOIN transactional_providers prv2 ON prv2.gen2providerid = (
        CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(med.administeredbygen2providerid, ''))) THEN med.administeredbygen2providerid
        WHEN 0 <> LENGTH(TRIM(COALESCE(med.gen2providerid, ''))) THEN med.gen2providerid
        ELSE med.hv_gen2providerid
        END
        )
    LEFT JOIN transactional_clients clt ON med.genclientid = clt.genclientid
    CROSS JOIN medication_exploder n
WHERE med.gen2patientid IS NOT NULL
    AND (
        ARRAY(
            CASE WHEN med.cvx != '0' THEN med.cvx END,
            CASE WHEN med.rxnorm != '0' THEN med.rxnorm END,
            CASE WHEN med.gpi != '0' THEN med.gpi END,
            CASE WHEN med.ddi != '0' THEN med.ddi END
            )[n.n] IS NOT NULL
        OR (
            COALESCE(
                CASE WHEN med.cvx != '0' THEN med.cvx END,
                CASE WHEN med.rxnorm != '0' THEN med.rxnorm END,
                CASE WHEN med.gpi != '0' THEN med.gpi END,
                CASE WHEN med.ddi != '0' THEN med.ddi END
                ) IS NULL AND n.n = 0
            )
        )
