SELECT
--    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    --  hv_diag_id
    --------------------------------------------------------------------------------------------------
    CONCAT('136_', enc.encounter_id)									                    AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	439                                                                                     AS hvm_vdr_id,
	136                                                                                     AS hvm_vdr_feed_id,
    ptn.practice_id                                                                         AS vdr_org_id,
	enc.encounter_id																		AS vdr_diag_id,
	CASE
	    WHEN enc.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID'
	    ELSE NULL
	END																		                AS vdr_diag_id_qual,
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN pay.hvid       IS NOT NULL THEN pay.hvid
        WHEN ptn.patient_id IS NOT NULL THEN CONCAT('136_' , ptn.patient_id)
        ELSE NULL
    END																			            AS hvid,
    --------------------------------------------------------------------------------------------------
    --  ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH
	    (
	        VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(COALESCE(enc.created_at, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE), COALESCE(ptn.birth_year, pay.yearofbirth)),
	        CAST(EXTRACT_DATE(COALESCE(enc.created_at, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE),
	        COALESCE(ptn.birth_year, pay.yearofbirth)


	    )																					AS ptnt_birth_yr,
     --------------------------------------------------------------------------------------------------
    --  ptnt_gender_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(ptn.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(ptn.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				    	AS ptnt_gender_cd,
    --------------------------------------------------------------------------------------------------
    --  ptnt_state_cd
    --------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE
	    (
	        CASE
	            WHEN LOCATE(' OR ', UPPER(ptn.state)) <> 0 THEN NULL
	            WHEN LOCATE(' OR ', UPPER(pay.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(ptn.state, pay.state, '')), 1, 2)
	        END
	    )																					AS ptnt_state_cd,
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip_cd
    --------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE
	    (
	        CASE
	            WHEN LOCATE (' OR ', UPPER(ptn.zip)) <> 0            THEN '000'
	            WHEN LOCATE (' OR ', UPPER(pay.threedigitzip)) <> 0  THEN '000'
	            ELSE SUBSTR(COALESCE(ptn.zip, pay.threedigitzip), 1, 3)
	        END
	    )																					AS ptnt_zip3_cd,
    --------------------------------------------------------------------------------------------------
    -- hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT
       (
         '136', '_',
        COALESCE(enc.encounter_id, 'NO_ENCOUNTER_ID'), '_',
        COALESCE(trs.transcript_id, 'NO_TRANSCRIPT_ID')
       )                                                                                   AS hv_enc_id,
    --------------------------------------------------------------------------------------------------
    --  enc_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(trs.dos, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
    --------------------------------------------------------------------------------------------------
    --  diag_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.created_at, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_dt,
    --------------------------------------------------------------------------------------------------
    --  diag_prov_vdr_id and diag_prov_vdr_id_qual
    --------------------------------------------------------------------------------------------------
	prv.provider_id  																        AS diag_prov_vdr_id,
	CASE
	    WHEN prv.provider_id IS NOT NULL THEN 'RENDERING_PROVIDER'
	    ELSE NULL
	END																		                AS diag_prov_vdr_id_qual,
    --------------------------------------------------------------------------------------------------
    --  diag_prov_nucc_taxnmy_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_ama_taxonomy IS NOT NULL  AND  UPPER(SUBSTR(prv.derived_ama_taxonomy,1,1)) <> 'X' THEN prv.derived_ama_taxonomy
	    WHEN spc.npi_classification IS NOT NULL    AND  UPPER(SUBSTR(spc.npi_classification,1,1)) <> 'X'   THEN spc.npi_classification
	    ELSE NULL
	END																					    AS diag_prov_nucc_taxnmy_cd,
    --------------------------------------------------------------------------------------------------
    --  diag_prov_alt_speclty_id and diag_prov_alt_speclty_id_qual
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_specialty IS NOT NULL  AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN prv.derived_specialty
	    WHEN spc.name IS NOT NULL               AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN spc.name
	    ELSE NULL
	END	                                                  							        AS diag_prov_alt_speclty_id,

	CASE
	    WHEN prv.derived_specialty IS NOT NULL AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN 'PROVIDER_DERIVED_SPECIALTY'
	    WHEN spc.name IS NOT NULL              AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN 'SPECIALTY_NAME'
	    ELSE NULL
	END			    																		AS diag_prov_alt_speclty_id_qual,
    --------------------------------------------------------------------------------------------------
    --  diag_prov_state_cd
    --------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE(
	        CASE
	            WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2)
	        END
    	)																					AS diag_prov_state_cd,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_zip_cd
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 THEN '000'
        ELSE SUBSTR(prc.zip, 1, 3)
    END																					    AS diag_prov_zip_cd,
    --------------------------------------------------------------------------------------------------
    --  diag_onset_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.created_at, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_onset_dt,

    --------------------------------------------------------------------------------------------------
    --  diag_resltn_dt
    --------------------------------------------------------------------------------------------------
    CAST (NULL AS DATE)																		AS diag_resltn_dt,
    --------------------------------------------------------------------------------------------------
    --  diag_cd and diag_cd_qual
    --------------------------------------------------------------------------------------------------
    CLEAN_UP_DIAGNOSIS_CODE
    (
        etp.code,
        CASE
            WHEN UPPER(etp.code_name) = 'ICD-9-CM'     THEN '01'
            WHEN UPPER(etp.code_name) = 'ICD-10-CM'    THEN '02'
            ELSE NULL
        END,
        CAST(EXTRACT_DATE(enc.created_at, '%Y-%m-%d') AS DATE)
    )                                                                                       AS diag_cd,

    CASE
        WHEN UPPER(etp.code_name) = 'ICD-9-CM'     THEN '01'
        WHEN UPPER(etp.code_name) = 'ICD-10-CM'    THEN '02'
        ELSE NULL
	END 																					AS diag_cd_qual,

    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.created_at, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,

	'encounter'																	            AS prmy_src_tbl_nm,
	'136'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(COALESCE(enc.created_at, SUBSTR(trs.dos, 1, 10)), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(enc.created_at, SUBSTR(trs.dos, 1, 10)), 1, 7)
	END																					    AS part_mth
    ,right(nvl(enc.encounter_id,'0'), 1) AS vdr_diag_id_key
FROM encounter enc
LEFT OUTER JOIN enctype    etp ON COALESCE(enc.enctype_id, 'NULL')    = COALESCE(etp.enctype_id, 'empty')
LEFT OUTER JOIN enccat     ect ON COALESCE(etp.enccat_id, 'NULL')     = COALESCE(ect.enccat_id, 'empty')
LEFT OUTER JOIN transcript trs ON COALESCE(enc.transcript_id, 'NULL') = COALESCE(trs.transcript_id, 'empty')
LEFT OUTER JOIN patient    ptn ON COALESCE(trs.patient_id, 'NULL')    = COALESCE(ptn.patient_id, 'empty')
LEFT OUTER JOIN provider   prv ON COALESCE(trs.provider_id, 'NULL')   = COALESCE(prv.provider_id, 'empty')
LEFT OUTER JOIN practice   prc ON COALESCE(prv.practice_id, 'NULL')   = COALESCE(prc.practice_id, 'empty')
LEFT OUTER JOIN specialty  spc ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
LEFT OUTER JOIN matching_payload    pay ON LOWER(COALESCE(ptn.patient_id, 'NULL'))    = LOWER(COALESCE(pay.claimid, 'empty'))

WHERE TRIM(UPPER(COALESCE(enc.encounter_id, 'empty'))) <> 'ENCOUNTER_ID'
  AND UPPER(etp.code_name) IN ( 'ICD-10-CM', 'ICD-9-CM')


-- diagnosis_icd10
-- diagnosis_icd9
-- matching_payload
-- diagnosis_snomed
