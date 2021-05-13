SELECT 
    /* hv_lab_test_id */
    CONCAT
        (
            '136|', 
            COALESCE(txn.laborder_id, 'NO_LABORDER_ID')
        )                                                                                    AS hv_lab_test_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '01a'                                                                                   AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.laborder_id                                                                            AS vdr_lab_ord_id,
    /* vdr_lab_ord_id_qual */
    CASE 
        WHEN txn.laborder_id IS NOT NULL 
            THEN 'LABORDER_ID' 
        ELSE NULL 
    END                                                                                        AS vdr_lab_ord_id_qual,
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
            CAST(EXTRACT_DATE(COALESCE(txn.report_date, txn.observed_at), '%Y-%m-%d') AS DATE),
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
    /* lab_test_execd_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.observed_at, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS lab_test_execd_dt,
    /* lab_result_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.report_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS lab_result_dt,
    /* lab_test_prov_qual */
    CASE 
        WHEN txn.vendor_id IS NOT NULL
            THEN 'EXECUTING_FACILITY' 
        ELSE NULL 
    END                                                                                        AS lab_test_prov_qual,
    txn.vendor_id                                                                            AS lab_test_prov_vdr_id,
    /* lab_test_prov_vdr_id_qual */
    CASE 
        WHEN txn.vendor_id IS NOT NULL
            THEN 'LABORDER.VENDOR_ID' 
        ELSE NULL 
    END                                                                                        AS lab_test_prov_vdr_id_qual,
    NULL                                                                                    AS lab_test_prov_nucc_taxnmy_cd,
    NULL                                                                                    AS lab_test_prov_alt_speclty_id,
    NULL                                                                                    AS lab_test_prov_alt_speclty_id_qual,
    UPPER(vdr.name)                                                                         AS lab_test_prov_fclty_nm,
    NULL                                                                                    AS lab_test_prov_state_cd,
    NULL                                                                                    AS lab_test_prov_zip_cd,
    CLEAN_UP_LOINC_CODE(txn.loinc_num)                                                      AS lab_test_loinc_cd,
    COALESCE(txn.obs_quan, txn.obs_qual)                                                    AS lab_result_msrmt,
    txn.unit                                                                                AS lab_result_uom,
    /* lab_result_abnorm_flg */
    CASE 
        WHEN SUBSTR(UPPER(COALESCE(txn.is_abnormal, 'X')), 1, 1) IN ('T', 'Y', '1') 
            THEN 'Y' 
        WHEN SUBSTR(UPPER(COALESCE(txn.is_abnormal, 'X')), 1, 1) IN ('F', 'N', '0')
            THEN 'N' 
        ELSE NULL 
    END                                                                                     AS lab_result_abnorm_flg,
    txn.result_status                                                                       AS lab_test_stat_cd,
    /* lab_test_stat_cd_qual */
    CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(txn.result_status, ''))) 
            THEN 'RESULT_STATUS' 
        ELSE NULL
    END                                                                                     AS lab_test_stat_cd_qual,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'laborder'                                                                                AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(txn.report_date, '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(txn.report_date, 1, 7)
    END                                                                                     AS part_mth
 FROM laborder txn
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN matching_payload pay
   ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
 LEFT OUTER JOIN vendor vdr
   ON LOWER(COALESCE(txn.vendor_id, 'NULL')) = COALESCE(vdr.vendor_id, 'empty')
WHERE TRIM(UPPER(COALESCE(txn.laborder_id, 'empty'))) <> 'LABORDER_ID'
  /* Per Veradigm, ignore rows without a LOINC code. */
  AND 0 <> LENGTH(TRIM(COALESCE(txn.loinc_num, '')))
  /* Only load rows that will add vendor info. */
  AND 0 <> LENGTH(TRIM(COALESCE(txn.vendor_id, '')))
