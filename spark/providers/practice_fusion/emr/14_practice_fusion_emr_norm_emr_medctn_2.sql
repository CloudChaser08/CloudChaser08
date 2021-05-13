SELECT 
    /* hv_medctn_id */
    CONCAT
        (
            '136|', 
            COALESCE(txn.prescription_id, 'NO_PRESCRIPTION_ID')
        )                                                                                    AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    '09a'                                                                                   AS mdl_vrsn_num,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.prescription_id                                                                        AS vdr_medctn_ord_id,
    /* vdr_medctn_ord_id_qual */
    CASE 
        WHEN txn.prescription_id IS NOT NULL 
            THEN 'PRESCRIPTION_ID' 
        ELSE NULL 
    END                                                                                        AS vdr_medctn_ord_id_qual,
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
            CAST(EXTRACT_DATE(txn.dos, '%Y-%m-%d') AS DATE),
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
    /* medctn_ord_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(txn.dos, txn.start_date), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS medctn_ord_dt,
    /* medctn_prov_qual */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
            THEN ARRAY
                    (
                        'ORDERING_PROVIDER',
                        'RENDERING_FACILITY'
                    )[prov_explode.n]
        ELSE NULL 
    END                                                                                        AS medctn_prov_qual,
    /* medctn_prov_vdr_id */
    ARRAY
        (
            txn.provider_id,
            txn.pharmacy_id
        )[prov_explode.n]                                                                   AS medctn_prov_vdr_id,
    /* medctn_prov_vdr_id_qual */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
            THEN ARRAY
                    (
                        'PRESCRIPTION.PROVIDER_ID',
                        'PRESCRIPTION.PHARMACY_ID'
                    )[prov_explode.n]
        ELSE NULL
    END                                                                                        AS medctn_prov_vdr_id_qual,
    /* medctn_prov_nucc_taxnmy_cd */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
         AND prov_explode.n = 0
            THEN
                CASE 
                    WHEN UPPER(COALESCE(prv.derived_ama_taxonomy, 'X')) <> 'X'
                        THEN prv.derived_ama_taxonomy 
                    WHEN UPPER(COALESCE(spc.npi_classification, 'X')) <> 'X' 
                        THEN spc.npi_classification 
                    ELSE NULL
                END
        ELSE NULL
    END                                                                                        AS medctn_prov_nucc_taxnmy_cd,
    /* medctn_prov_alt_speclty_id */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
         AND prov_explode.n = 0
            THEN UPPER(COALESCE(prv.derived_specialty, spc.name))
        ELSE NULL
    END                                                                                     AS medctn_prov_alt_speclty_id,
    /* medctn_prov_alt_speclty_id_qual */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
         AND prov_explode.n = 0
         AND COALESCE(prv.derived_specialty, spc.name) IS NOT NULL 
            THEN 'DERIVED_SPECIALTY'
        ELSE NULL
    END                                                                                        AS medctn_prov_alt_speclty_id_qual,
    /* medctn_prov_fclty_nm */
    CASE
        WHEN ARRAY
                (
                    txn.provider_id,
                    txn.pharmacy_id
                )[prov_explode.n] IS NOT NULL
         AND prov_explode.n = 1
            THEN phy.name
        ELSE NULL
    END                                                                                     AS medctn_prov_fclty_nm,
    /* medctn_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN LOCATE(' OR ', UPPER(COALESCE(ARRAY
                                                    (
                                                        prc.state,
                                                        phy.state
                                                    )[prov_explode.n], ''))) <> 0 
                    THEN NULL
                ELSE SUBSTR(UPPER(COALESCE(ARRAY
                                            (
                                                prc.state,
                                                phy.state
                                            )[prov_explode.n], '')), 1, 2)
            END
        )                                                                                    AS medctn_prov_state_cd,
    /* medctn_prov_zip_cd */
    CASE 
        WHEN LOCATE(' OR ', UPPER(COALESCE(ARRAY
                                            (
                                                prc.zip,
                                                phy.zip
                                            )[prov_explode.n], ''))) <> 0 
            THEN '000' 
        ELSE SUBSTR(ARRAY
                        (
                            prc.zip,
                            phy.zip
                        )[prov_explode.n], 1, 3)
    END                                                                                        AS medctn_prov_zip_cd,
    /* medctn_start_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.start_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS medctn_start_dt,
    /* medctn_end_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.stop_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS medctn_end_dt,
    /* medctn_diag_cd */
    CLEAN_UP_DIAGNOSIS_CODE
        (
            d09.icd9,
            '01',
            CAST(EXTRACT_DATE(txn.dos, '%Y-%m-%d') AS DATE)
        )                                                                                   AS medctn_diag_cd,
    /* medctn_diag_cd_qual */
    CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(d09.icd9, '')))
            THEN '01'
        ELSE NULL
    END                                                                                     AS medctn_diag_cd_qual,
    CLEAN_UP_NDC_CODE(txn.medication_id)                                                    AS medctn_ndc,
    med.rxnorm_cui                                                                          AS medctn_alt_cd,
    /* medctn_alt_cd_qual */
    CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(med.rxnorm_cui, '')))
            THEN 'RXNORM'
        ELSE NULL
    END                                                                                     AS medctn_alt_cd_qual,
    /* medctn_genc_ok_flg */
    CASE
        WHEN SUBSTR(UPPER(COALESCE(txn.dispensed_as_written, 'X')), 1, 1) IN ('T', 'Y', '1')
            THEN 'N' 
        WHEN SUBSTR(UPPER(COALESCE(txn.dispensed_as_written, 'X')), 1, 1) IN ('F', 'N', '0')
            THEN 'Y'
        ELSE NULL 
    END                                                                                     AS medctn_genc_ok_flg,
    med.trade_name                                                                          AS medctn_brd_nm,
    med.generic_name                                                                        AS medctn_genc_nm,
    /* medctn_rx_flg */
    CASE 
        WHEN UPPER(COALESCE(med.rx_or_otc, '')) IN ('R', 'S') THEN 'Y' 
        WHEN UPPER(COALESCE(med.rx_or_otc, '')) IN ('O', 'P') THEN 'N' 
        ELSE NULL 
    END                                                                                     AS medctn_rx_flg,
    COALESCE(txn.quantity, txn.c_quantity)                                                  AS medctn_rx_qty,
    COALESCE(txn.daily_admin, txn.c_daily_admin)                                            AS medctn_dly_qty,
    txn.days_supply                                                                         AS medctn_days_supply_qty,
    COALESCE(txn.unit, txn.c_quantity_unit)                                                 AS medctn_admin_form_nm,
    med.strength                                                                            AS medctn_strth_txt,
    /* medctn_dose_txt */
    CASE 
        WHEN 0 <> LENGTH
                    (
                        CONCAT
                            (
                                TRIM(COALESCE(txn.c_dose_amount, '')),
                                TRIM(COALESCE(txn.c_dose_amount_unit, ''))
                            )
                    )
            THEN TRIM(CONCAT(' ', COALESCE(txn.c_dose_amount, ''), ' ', COALESCE(txn.c_dose_amount_unit, '')))
        ELSE NULL
    END                                                                                     AS medctn_dose_txt,
    med.route                                                                               AS medctn_admin_rte_txt,
    txn.num_refills                                                                         AS medctn_remng_rfll_qty,
    /* medctn_elect_rx_flg */
    CASE 
        WHEN SUBSTR(UPPER(COALESCE(txn.erx, 'X')), 1, 1) IN ('T', 'Y', '1')
            THEN 'Y' 
        WHEN SUBSTR(UPPER(COALESCE(txn.erx, 'X')), 1, 1) IN ('F', 'N', '0')
            THEN 'N' 
        ELSE NULL 
    END                                                                                     AS medctn_elect_rx_flg,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'prescription'                                                                            AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(COALESCE(txn.dos, txn.start_date), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(COALESCE(txn.dos, txn.start_date), 1, 7)
    END                                                                                     AS part_mth
 FROM prescription txn
 LEFT OUTER JOIN medication med
   ON COALESCE(txn.medication_id, 'NULL') = COALESCE(med.medication_id, 'empty')
 LEFT OUTER JOIN diagnosis_icd9 d09
   ON COALESCE(txn.diagnosis_id, CONCAT('NULL', txn.prescription_id)) = COALESCE(d09.diagnosis_id, 'empty')
 LEFT OUTER JOIN pharmacy phy
   ON COALESCE(txn.pharmacy_id, 'NULL') = COALESCE(phy.pharmacy_id, 'empty')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN provider prv
   ON COALESCE(txn.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
 LEFT OUTER JOIN practice prc
   ON COALESCE(prv.practice_id, 'NULL') = COALESCE(prc.practice_id, 'empty')
 LEFT OUTER JOIN specialty spc
   ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
 LEFT OUTER JOIN matching_payload pay
   ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) prov_explode
WHERE TRIM(UPPER(COALESCE(txn.prescription_id, 'empty'))) <> 'PRESCRIPTION_ID'
  AND
    (
        (
            COALESCE(txn.provider_id, txn.pharmacy_id) IS NULL
        AND prov_explode.n = 0
        )
     OR ARRAY
        (
            txn.provider_id,
            txn.pharmacy_id
        )[prov_explode.n] IS NOT NULL
    )
  /* Load ONLY where there is an ICD-9 code. */
  AND 0 <> LENGTH(TRIM(COALESCE(d09.icd9, '')))
