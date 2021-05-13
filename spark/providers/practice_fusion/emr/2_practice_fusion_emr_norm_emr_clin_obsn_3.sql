SELECT 
    /* hv_clin_obsn_id */
    CONCAT
        (
            '136|',
            COALESCE(txn.patient_smoke_id, 'NO_PATIENT_SMOKE_ID')
        )                                                                                    AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '09'                                                                                    AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.patient_smoke_id                                                                    AS vdr_clin_obsn_id,
    /* vdr_clin_obsn_id_qual */
    CASE 
        WHEN txn.patient_smoke_id IS NOT NULL 
            THEN 'PATIENT_SMOKE_ID' 
        ELSE NULL 
    END                                                                                        AS vdr_clin_obsn_id_qual,
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
            CAST(EXTRACT_DATE(txn.effective_date, '%Y-%m-%d') AS DATE),
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
    NULL                                                                                    AS hv_enc_id,
    NULL                                                                                    AS enc_dt,
    /* clin_obsn_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.effective_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS clin_obsn_dt,
    NULL                                                                                    AS clin_obsn_prov_qual,
    NULL                                                                                    AS clin_obsn_prov_vdr_id,
    NULL                                                                                    AS clin_obsn_prov_vdr_id_qual,
    NULL                                                                                    AS clin_obsn_prov_nucc_taxnmy_cd,
    NULL                                                                                    AS clin_obsn_prov_alt_speclty_id,
    NULL                                                                                    AS clin_obsn_prov_alt_speclty_id_qual,
    NULL                                                                                    AS clin_obsn_prov_state_cd,
    NULL                                                                                    AS clin_obsn_prov_zip_cd,
    NULL                                                                                    AS clin_obsn_onset_dt,
    NULL                                                                                    AS clin_obsn_resltn_dt,
    'SMOKING_HISTORY'                                                                       AS clin_obsn_typ_cd,
    NULL                                                                                    AS clin_obsn_ndc,
    NULL                                                                                    AS clin_obsn_diag_nm,
    smk.description                                                                         AS clin_obsn_msrmt,
    NULL                                                                                    AS clin_obsn_uom,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'patient_smoke'                                                                            AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(txn.effective_date, '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(txn.effective_date, 1, 7)
    END                                                                                     AS part_mth
 FROM patient_smoke txn
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN smoke smk
   ON COALESCE(txn.smoke_id, 'NULL') = COALESCE(smk.smoke_id, 'empty')
 LEFT OUTER JOIN matching_payload pay
   ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
WHERE TRIM(UPPER(COALESCE(txn.patient_id, 'empty'))) <> 'PATIENT_ID'
