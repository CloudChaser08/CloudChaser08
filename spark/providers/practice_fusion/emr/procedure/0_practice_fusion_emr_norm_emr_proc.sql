SELECT 
    monotonically_increasing_id()                                                           AS row_id,
    /* hv_proc_id */
    CONCAT
        (
            '136|', 
            COALESCE(txn.vaccination_id, 'NO_VACCINATION_ID')
        )                                                                                    AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '10'                                                                                    AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.vaccination_id                                                                        AS vdr_proc_id,
    /* vdr_proc_id_qual */
    CASE 
        WHEN txn.vaccination_id IS NOT NULL 
            THEN 'VACCINATION_ID' 
        ELSE NULL 
    END                                                                                        AS vdr_proc_id_qual,
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
            CAST(EXTRACT_DATE(txn.date_administered, '%Y-%m-%d') AS DATE),
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
    /* proc_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_administered, '%Y-%m-%d') AS DATE),
            eddt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS proc_dt,
    /* proc_prov_qual */
    CASE 
        WHEN txn.administered_provider_id IS NOT NULL
            THEN 'RENDERING_PROVIDER' 
        ELSE NULL 
    END                                                                                        AS proc_prov_qual,
    txn.administered_provider_id                                                            AS proc_prov_vdr_id,
    /* proc_prov_vdr_id_qual */
    CASE 
        WHEN txn.administered_provider_id IS NOT NULL
            THEN 'VACCINATION.ADMINISTERED_PROVIDER_ID' 
        ELSE NULL 
    END                                                                                        AS proc_prov_vdr_id_qual,
    /* proc_prov_nucc_taxnmy_cd */
    CASE 
        WHEN UPPER(COALESCE(prv.derived_ama_taxonomy, 'X')) <> 'X'
            THEN prv.derived_ama_taxonomy 
        WHEN UPPER(COALESCE(spc.npi_classification, 'X')) <> 'X' 
            THEN spc.npi_classification 
        ELSE NULL 
    END                                                                                        AS proc_prov_nucc_taxnmy_cd,
    UPPER(COALESCE(prv.derived_specialty, spc.name))                                        AS proc_prov_alt_speclty_id,
    /* proc_prov_alt_speclty_id_qual */
    CASE
        WHEN COALESCE(prv.derived_specialty, spc.name) IS NOT NULL 
            THEN 'DERIVED_SPECIALTY'
        ELSE NULL
    END                                                                                        AS proc_prov_alt_speclty_id_qual,
    /* proc_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 
                    THEN NULL
                ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2) 
            END
        )                                                                                    AS proc_prov_state_cd,
    /* proc_prov_zip_cd */
    CASE 
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 
            THEN '000' 
        ELSE SUBSTR(prc.zip, 1, 3) 
    END                                                                                        AS proc_prov_zip_cd,
    UPPER(vxn.cvx_code)                                                                     AS proc_cd,
    /* proc_cd_qual */
    CASE
        WHEN 0 <> LENGTH(TRIM(COALESCE(vxn.cvx_code, '')))
            THEN 'CVX'
        ELSE NULL
    END                                                                                     AS proc_cd_qual,
    txn.vaccine_id                                                                          AS proc_alt_cd,
    /* proc_alt_cd_qual */
    CASE
        WHEN txn.vaccine_id IS NOT NULL
            THEN 'VACCINE_ID'
        ELSE NULL
    END                                                                                     AS proc_alt_cd_qual,
    /* proc_stat_cd */
    CASE 
        WHEN 0 <> LENGTH(TRIM(CONCAT(COALESCE(txn.status, ''), COALESCE(txn.rejection_reason, '')))) 
            THEN SUBSTR
                    (
                        CONCAT
                            (
                                CASE 
                                    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.status, ''))) 
                                        THEN CONCAT
                                                (
                                                    ' | STATUS: ', 
                                                    CASE
                                                        WHEN txn.status = '1' THEN 'Initiated'
                                                        WHEN txn.status = '2' THEN 'Pending'
                                                        WHEN txn.status = '3' THEN 'Fulfilled'
                                                        WHEN txn.status = '4' THEN 'Canceled'
                                                        ELSE txn.status
                                                    END
                                                )
                                    ELSE ''
                                END, 
                                CASE 
                                    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.rejection_reason, ''))) 
                                        THEN CONCAT
                                                (
                                                    ' | REJECTION REASON: ', 
                                                    CASE
                                                        WHEN txn.rejection_reason = '1' THEN 'Parental Decision'
                                                        WHEN txn.rejection_reason = '2' THEN 'Religious Exemption'
                                                        WHEN txn.rejection_reason = '3' THEN 'Other'
                                                        WHEN txn.rejection_reason = '4' THEN 'Patient Decision'
                                                        WHEN txn.rejection_reason = '5' THEN 'Immunity' 
                                                        WHEN txn.rejection_reason = '6' THEN 'Medical Precaution' 
                                                        WHEN txn.rejection_reason = '7' THEN 'Out of Stock' 
                                                        WHEN txn.rejection_reason = '8' THEN 'Patient Objection' 
                                                        WHEN txn.rejection_reason = '9' THEN 'Philosophical Objection'
                                                        WHEN txn.rejection_reason = '10' THEN 'Religious Objection' 
                                                        WHEN txn.rejection_reason = '11' THEN 'Vaccine Efficacy Concerns'
                                                        WHEN txn.rejection_reason = '12' THEN 'Vaccine Safety Concerns' 
                                                        ELSE txn.rejection_reason
                                                    END
                                                )
                                    ELSE ''
                                END
                            ), 4
                    )
        ELSE NULL 
    END                                                                                     AS proc_stat_cd,
    'VACCINATION'                                                                           AS proc_typ_cd,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.created_at, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'vaccination'                                                                            AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(txn.date_administered, '%Y-%m-%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(txn.date_administered, 1, 7)
    END                                                                                        AS part_mth
 FROM vaccination txn
 LEFT OUTER JOIN vaccine vxn
   ON COALESCE(txn.vaccine_id, 'NULL') = COALESCE(vxn.vaccine_id, 'empty')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN provider prv
   ON COALESCE(txn.administered_provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
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
 LEFT OUTER JOIN ref_gen_ref eddt
   ON 1 = 1
  AND eddt.hvm_vdr_feed_id = 136
  AND eddt.gen_ref_domn_nm = 'EARLIEST_VALID_DIAGNOSIS_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 136
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
WHERE TRIM(UPPER(COALESCE(txn.vaccination_id, 'empty'))) <> 'VACCINATION_ID'
