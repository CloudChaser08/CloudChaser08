SELECT 
    /* hv_enc_id */
    CONCAT
        (
            '136|', 
            COALESCE(txn.transcript_id, 'NO_TRANSCRIPT_ID'), 
            '|', 
            COALESCE(txn.encounter_id, 'NO_ENCOUNTER_ID')
        )                                                                                    AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '08'                                                                                    AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.encounter_id                                                                        AS vdr_enc_id,
    /* vdr_enc_id_qual */
    CASE 
        WHEN txn.encounter_id IS NOT NULL 
            THEN 'ENCOUNTER_ID' 
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
            CAST(EXTRACT_DATE(trs.dos, '%Y-%m-%d') AS DATE),
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
            CAST(EXTRACT_DATE(trs.dos, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS enc_start_dt,
    NULL                                                                                     AS enc_end_dt,
    /* enc_prov_qual */
    CASE 
        WHEN COALESCE
                (
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
            THEN 'RENDERING_PROVIDER' 
        ELSE NULL 
    END                                                                                        AS enc_prov_qual,
    trs.provider_id                                                                            AS enc_prov_vdr_id,
    /* enc_prov_vdr_id_qual */
    CASE 
        WHEN trs.provider_id IS NOT NULL
            THEN 'TRANSCRIPT.PROVIDER_ID' 
        ELSE NULL 
    END                                                                                        AS enc_prov_vdr_id_qual,
    NULL                                                                                       AS enc_prov_alt_id,
    NULL                                                                                       AS enc_prov_alt_id_qual,
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
    UPPER(ect.name)                                                                         AS enc_typ_nm,
    NULL                                                                                    AS enc_stat_cd,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(trs.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'encounter'                                                                                AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(trs.dos, '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(trs.dos, 1, 7)
    END                                                                                        AS part_mth
 FROM encounter txn
 LEFT OUTER JOIN enctype etp
   ON COALESCE(txn.enctype_id, 'NULL') = COALESCE(etp.enctype_id, 'empty')
 LEFT OUTER JOIN enccat ect
   ON COALESCE(etp.enccat_id, 'NULL') = COALESCE(ect.enccat_id, 'empty')
 LEFT OUTER JOIN transcript trs
   ON COALESCE(txn.transcript_id, 'NULL') = COALESCE(trs.transcript_id, 'empty')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(trs.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN provider prv
   ON COALESCE(trs.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
 LEFT OUTER JOIN practice prc
   ON COALESCE(prv.practice_id, 'NULL') = COALESCE(prc.practice_id, 'empty')
 LEFT OUTER JOIN specialty spc
   ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
 LEFT OUTER JOIN matching_payload pay
   ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
WHERE TRIM(UPPER(COALESCE(txn.encounter_id, 'empty'))) <> 'ENCOUNTER_ID'
