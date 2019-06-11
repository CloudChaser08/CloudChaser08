SELECT 
    /* hv_clin_obsn_id */
    CONCAT
        (
            '136|',
             COALESCE(asd.transcript_id, 'NO_TRANSCRIPT_ID'),
             '|',
            COALESCE(txn.allergy_id, 'NO_ALLERGY_ID')
        )                                                                                    AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/')) - 1]              AS data_set_nm,
    '09'                                                                                    AS mdl_vrsn_num,
    439                                                                                     AS hvm_vdr_id,
    136                                                                                     AS hvm_vdr_feed_id,
    txn.allergy_id                                                                            AS vdr_clin_obsn_id,
    /* vdr_clin_obsn_id_qual */
    CASE 
        WHEN txn.allergy_id IS NOT NULL 
            THEN 'ALLERGY_ID' 
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
            CAST(EXTRACT_DATE(COALESCE(txn.start_date, asd.dos), '%Y-%m-%d') AS DATE),
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
            CAST(EXTRACT_DATE(COALESCE(txn.start_date, asd.dos), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS clin_obsn_dt,
    /* clin_obsn_prov_qual */
    CASE 
        WHEN asd.provider_id IS NOT NULL
            THEN 'RENDERING_PROVIDER' 
        ELSE NULL 
    END                                                                                        AS clin_obsn_prov_qual,
    asd.provider_id                                                                            AS clin_obsn_prov_vdr_id,
    /* clin_obsn_prov_vdr_id_qual */
    CASE 
        WHEN asd.provider_id IS NOT NULL
            THEN 'TRANSCRIPT.PROVIDER_ID' 
        ELSE NULL 
    END                                                                                        AS clin_obsn_prov_vdr_id_qual,
    /* clin_obsn_prov_nucc_taxnmy_cd */
    CASE 
        WHEN UPPER(COALESCE(prv.derived_ama_taxonomy, 'X')) <> 'X'
            THEN prv.derived_ama_taxonomy 
        WHEN UPPER(COALESCE(spc.npi_classification, 'X')) <> 'X' 
            THEN spc.npi_classification 
        ELSE NULL 
    END                                                                                        AS clin_obsn_prov_nucc_taxnmy_cd,
    UPPER(COALESCE(prv.derived_specialty, spc.name))                                        AS clin_obsn_prov_alt_speclty_id,
    /* clin_obsn_prov_alt_speclty_id_qual */
    CASE
        WHEN COALESCE(prv.derived_specialty, spc.name) IS NOT NULL 
            THEN 'DERIVED_SPECIALTY'
        ELSE NULL
    END                                                                                        AS clin_obsn_prov_alt_speclty_id_qual,
    /* clin_obsn_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 
                    THEN NULL
                ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2) 
            END
        )                                                                                    AS clin_obsn_prov_state_cd,
    /* clin_obsn_prov_zip_cd */
    CASE 
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 
            THEN '000' 
        ELSE SUBSTR(prc.zip, 1, 3) 
    END                                                                                        AS clin_obsn_prov_zip_cd,
    /* clin_obsn_onset_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(txn.start_date, asd.dos), '%Y-%m-%d') AS DATE),
            eddt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS clin_obsn_onset_dt,
    /* clin_obsn_resltn_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.stop_date, '%Y-%m-%d') AS DATE),
            eddt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS clin_obsn_resltn_dt,
    /* clin_obsn_typ_cd */
    CASE 
        WHEN txn.medication_id IS NOT NULL
            THEN 'MEDICATION_ALLERGY' 
        WHEN txn.allergen_id IS NOT NULL 
            THEN 'SUBSTANCE_ALLERGY' 
        ELSE NULL 
    END                                                                                     AS clin_obsn_typ_cd,
    CLEAN_UP_NDC_CODE(txn.medication_id)                                                    AS clin_obsn_ndc,
    agn.description                                                                         AS clin_obsn_diag_nm,
    NULL                                                                                    AS clin_obsn_msrmt,
    NULL                                                                                    AS clin_obsn_uom,
    /* data_captr_dt */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.created_at, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                    AS data_captr_dt,
    'allergy'                                                                                AS prmy_src_tbl_nm,
    '136'                                                                                    AS part_hvm_vdr_feed_id,
    /* part_mth */
    CASE 
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(COALESCE(txn.start_date, asd.dos), '%Y-%m-%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
                ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE SUBSTR(COALESCE(txn.start_date, asd.dos), 1, 7)
    END                                                                                     AS part_mth
 FROM allergy txn
 LEFT OUTER JOIN allergen agn
   ON COALESCE(txn.allergen_id, 'NULL') = COALESCE(agn.allergen_id, 'empty')
 LEFT OUTER JOIN medication med
   ON COALESCE(txn.medication_id, 'NULL') = COALESCE(med.medication_id, 'empty')
 LEFT OUTER JOIN
(
    SELECT
        sub.allergy_id,
        sub.transcript_id,
        sub.dos,
        sub.provider_id
     FROM
    (
        SELECT
            tal.allergy_id,
            trs.transcript_id,
            trs.dos,
            trs.provider_id
         FROM transcript trs
         LEFT OUTER JOIN transcript_allergy tal
           ON COALESCE(tal.transcript_id, 'NULL') = COALESCE(trs.transcript_id, 'empty')
         INNER JOIN
         (
              SELECT tal2.allergy_id, MIN(trs2.dos) as dos
              FROM transcript trs2
	          LEFT OUTER JOIN transcript_allergy tal2
                      ON COALESCE(tal2.transcript_id, 'NULL') = COALESCE(trs2.transcript_id, 'empty')
              GROUP BY tal2.allergy_id
         ) earliest
           ON earliest.allergy_id = tal.allergy_id
               AND earliest.dos = trs.dos
    ) sub
) asd
   ON COALESCE(txn.allergy_id, 'NULL') = COALESCE(asd.allergy_id, 'empty')
 LEFT OUTER JOIN patient ptn
   ON COALESCE(txn.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
 LEFT OUTER JOIN provider prv
   ON COALESCE(asd.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
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
WHERE TRIM(UPPER(COALESCE(txn.allergy_id, 'empty'))) <> 'ALLERGY_ID'