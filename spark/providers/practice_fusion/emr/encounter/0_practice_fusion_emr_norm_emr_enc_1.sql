SELECT 
    /* hv_enc_id */
    CONCAT
        (
            '136|', 
            COALESCE(txn.transcript_id, 'NO_TRANSCRIPT_ID'), 
            '|', 
            COALESCE(txn.appointment_id, 'NO_APPOINTMENT_ID')
        )                                                                                    AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '08'                                                                                    AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.appointment_id                                                                        AS vdr_enc_id,
    /* vdr_enc_id_qual */
    CASE 
        WHEN txn.appointment_id IS NOT NULL 
            THEN 'APPOINTMENT_ID' 
        ELSE NULL 
    END                                                                                        AS vdr_enc_id_qual,
    /* hvid */
    CASE 
        WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, ''))) 
            THEN pay.hvid
        WHEN 0 <> LENGTH(TRIM(COALESCE(ptn.patient_id, ''))) 
            THEN CONCAT('136|', COALESCE(ptn.patient_id, 'NONE')) 
        ELSE NULL 
    END                                                                                        AS hvid,
    /* ptnt_birth_yr */
    CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.start_time, '%Y-%m-%d') AS DATE),
            COALESCE(ptn.birth_year, pay.yearofbirth)
        )                                                                                    AS ptnt_birth_yr,
    CASE 
        WHEN SUBSTR(UPPER(COALESCE(ptn.gender, pay.gender, 'U')), 1, 1) IN ('F', 'M') 
            THEN SUBSTR(UPPER(COALESCE(ptn.gender, pay.gender, 'U')), 1, 1) 
        ELSE 'U' 
    END                                                                                        AS ptnt_gender_cd,
    /* ptnt_state_cd */
    VALIDATE_STATE_CODE
        (
            CASE 
                WHEN LOCATE(' OR ', UPPER(ptn.state)) <> 0 
                    THEN NULL 
                WHEN LOCATE(' OR ', UPPER(pay.state)) <> 0 
                    THEN NULL 
                ELSE SUBSTR(UPPER(COALESCE(ptn.state, pay.state, '')), 1, 2) 
            END
        )                                                                                    AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            CASE 
                WHEN LOCATE (' OR ', UPPER(ptn.zip)) <> 0 
                    THEN '000' 
                WHEN LOCATE (' OR ', UPPER(pay.threedigitzip)) <> 0 
                    THEN '000' 
                ELSE SUBSTR(COALESCE(ptn.zip, pay.threedigitzip), 1, 3) 
            END
        )                                                                                    AS ptnt_zip3_cd,
    /* enc_start_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.start_time, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            /* Allow appointment dates to be up to 2 years in the future. */
            CAST
                (
                    CONCAT
                        (
                            CAST(2 + CAST(SUBSTR('{VDR_FILE_DT}', 1, 4) AS INTEGER) AS STRING),
                            SUBSTR('{VDR_FILE_DT}', 5, 10)
                        ) AS DATE
                )
        )                                                                                    AS enc_start_dt,
    /* enc_end_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.end_time, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            /* Allow appointment dates to be up to 2 years in the future. */
            CAST
                (
                    CONCAT
                        (
                            CAST(2 + CAST(SUBSTR('{VDR_FILE_DT}', 1, 4) AS INTEGER) AS STRING),
                            SUBSTR('{VDR_FILE_DT}', 5, 10)
                        ) AS DATE
                )
        )                                                                                    AS enc_end_dt,
    /* enc_prov_qual */
    CASE 
        WHEN COALESCE
                (
                    txn.provider_id,
                    trs.provider_id, 
                    prv.derived_ama_taxonomy, 
                    spc.npi_classification, 
                    prv.derived_specialty, 
                    spc.name
                ) IS NOT NULL
         OR 0 <> LENGTH(TRIM(COALESCE
                                (
                                    VALIDATE_STATE_CODE
                                        (
                                            CASE
                                                WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 
                                                    THEN NULL
                                                ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2) 
                                            END
                                        )
                                , '')))
         OR 0 <> LENGTH(TRIM(COALESCE
                                (
                                    MASK_ZIP_CODE
                                        (
                                            CASE 
                                                WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 
                                                    THEN '000' 
                                                ELSE SUBSTR(prc.zip, 1, 3) 
                                            END
                                        )
                                , '')))
            THEN 'APPOINTMENT_PROVIDER' 
        ELSE NULL 
    END                                                                                        AS enc_prov_qual,
    txn.provider_id                                                                            AS enc_prov_vdr_id,
    /* enc_prov_vdr_id_qual */
    CASE 
        WHEN txn.provider_id IS NOT NULL
            THEN 'APPOINTMENT.PROVIDER_ID' 
        ELSE NULL 
    END                                                                                        AS enc_prov_vdr_id_qual,
    trs.provider_id                                                                            AS enc_prov_alt_id,
    /* enc_prov_alt_id_qual */
    CASE 
        WHEN trs.provider_id IS NOT NULL 
            THEN 'TRANSCRIPT.PROVIDER_ID'
        ELSE NULL 
    END                                                                                        AS enc_prov_alt_id_qual,
    /* enc_prov_nucc_taxnmy_cd */
    CASE 
        WHEN UPPER(COALESCE(prv.derived_ama_taxonomy, 'X')) <> 'X'
            THEN prv.derived_ama_taxonomy 
        WHEN UPPER(COALESCE(spc.npi_classification, 'X')) <> 'X' 
            THEN spc.npi_classification 
        ELSE NULL 
    END                                                                                        AS enc_prov_nucc_taxnmy_cd,
    UPPER(COALESCE(prv.derived_specialty, spc.name))                                        AS enc_prov_alt_speclty_id,
    /* enc_prov_alt_speclty_id_qual */
    CASE
        WHEN COALESCE(prv.derived_specialty, spc.name) IS NOT NULL 
            THEN 'DERIVED_SPECIALTY'
        ELSE NULL
    END                                                                                        AS enc_prov_alt_speclty_id_qual,
    /* enc_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 
                    THEN NULL
                ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2) 
            END
        )                                                                                    AS enc_prov_state_cd,
    /* enc_prov_zip_cd */
    CASE 
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 
            THEN '000' 
        ELSE SUBSTR(prc.zip, 1, 3) 
    END                                                                                        AS enc_prov_zip_cd,
    UPPER(txn.appointment_type)                                                             AS enc_typ_nm,
    /* enc_stat_cd */
    CASE 
        WHEN SUBSTR(UPPER(COALESCE(txn.is_cancelled, 'N')), 1, 1) IN ('T', 'Y', '1')
            THEN 'Canceled'
        ELSE NULL 
    END                                                                                        AS enc_stat_cd,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(trs.last_modified, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'appointment'                                                                            AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(txn.start_time, '%Y-%m-%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    /* Allow appointment dates to be up to 2 years in the future. */
                    CAST
                        (
                            CONCAT
                                (
                                    CAST(2 + CAST(SUBSTR('{VDR_FILE_DT}', 1, 4) AS INTEGER) AS STRING),
                                    SUBSTR('{VDR_FILE_DT}', 5, 10)
                                ) AS DATE
                        )
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(txn.start_time, 1, 7)
    END                                                                                        AS part_mth
 FROM appointment txn
 LEFT OUTER JOIN transcript trs
   ON COALESCE(txn.transcript_id, 'NULL') = COALESCE(trs.transcript_id, 'empty')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN provider prv
   ON COALESCE(txn.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
 LEFT OUTER JOIN practice prc
   ON COALESCE(prv.practice_id, 'NULL') = COALESCE(prc.practice_id, 'empty')
 LEFT OUTER JOIN specialty spc
   ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
 LEFT OUTER JOIN payload pay
   ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
 LEFT OUTER JOIN ref_gen_ref esdt
   ON 1 = 1
  AND esdt.hvm_vdr_feed_id = 136
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 136
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
WHERE TRIM(UPPER(COALESCE(txn.appointment_id, 'empty'))) <> 'APPOINTMENT_ID'
  /* Ignore appointment rows where the provider_id is NOT in the provider table. */
  AND
    (
        txn.provider_id IS NULL
     OR EXISTS
        (
            SELECT 1
             FROM provider prv1
            WHERE COALESCE(txn.provider_id, 'NULL') = COALESCE(prv1.provider_id, 'empty')
        )
    )
