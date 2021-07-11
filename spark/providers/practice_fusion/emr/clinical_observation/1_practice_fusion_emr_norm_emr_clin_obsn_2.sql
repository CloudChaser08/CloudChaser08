SELECT
    -------------------------------------------------------------------------------------------------------------------------
    --  hv_lab_test_id
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT('136_', psm.patient_smoke_id)							                        AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(psm.input_file_name, '/')[SIZE(SPLIT(psm.input_file_name, '/')) - 1]              AS data_set_nm,
	439                                                                                     AS hvm_vdr_id,
	136                                                                                     AS hvm_vdr_feed_id,
    ptn.practice_id                                                                         AS vdr_org_id,
	psm.patient_smoke_id																	AS vdr_clin_obsn_id,
	CASE
	    WHEN psm.patient_smoke_id IS NOT NULL THEN 'PATIENT_SMOKE_ID'
	    ELSE NULL
	END																		                AS vdr_clin_obsn_id_qual,
    -------------------------------------------------------------------------------------------------------------------------
    --  hvid
    -------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN pay.hvid       IS NOT NULL THEN pay.hvid
        WHEN ptn.patient_id IS NOT NULL THEN CONCAT('136_' , ptn.patient_id)
        ELSE NULL
    END																			            AS hvid,

    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_birth_yr
    -------------------------------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH
	    (
	        VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(COALESCE(psm.effective_date, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE), COALESCE(ptn.birth_year, pay.yearofbirth)),
	        CAST(EXTRACT_DATE(COALESCE(psm.effective_date, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE),
	        COALESCE(ptn.birth_year, pay.yearofbirth)
	    )																					AS ptnt_birth_yr,
    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_gender_cd
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(ptn.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(ptn.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				    	AS ptnt_gender_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  ptnt_zip_cd
    -------------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE
	    (
	        CASE
	            WHEN LOCATE (' OR ', UPPER(ptn.zip)) <> 0            THEN '000'
	            WHEN LOCATE (' OR ', UPPER(pay.threedigitzip)) <> 0  THEN '000'
	            ELSE SUBSTR(COALESCE(ptn.zip, pay.threedigitzip), 1, 3)
	        END
	    )																					AS ptnt_zip3_cd,
    -------------------------------------------------------------------------------------------------------------------------
    -- hv_enc_id
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT
        (
            '136', '_',
            COALESCE(psm.patient_smoke_id, 'NO_SMOKING_ID'), '_',
            COALESCE(trs.transcript_id, 'NO_TRANSCRIPT_ID')
        )                                                                                   AS hv_enc_id,

    -------------------------------------------------------------------------------------------------------------------------
    --  enc_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(trs.dos, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  clin_obsn_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(psm.effective_date, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_dt,
    -------------------------------------------------------------------------------------------------------------------------
    --  clin_obsn_dt and clin_obsn_prov_vdr_id_qual
    -------------------------------------------------------------------------------------------------------------------------
	trs.provider_id																			AS clin_obsn_prov_vdr_id,
	CASE
	    WHEN trs.provider_id IS NOT NULL THEN 'TRANSCRIPT_PROVIDER_ID'
	    ELSE NULL
	END																                        AS clin_obsn_prov_vdr_id_qual,
    -------------------------------------------------------------------------------------------------------------------------
    --  proc_prov_nucc_taxnmy_cd
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_ama_taxonomy IS NOT NULL  AND  UPPER(SUBSTR(prv.derived_ama_taxonomy,1,1)) <> 'X' THEN prv.derived_ama_taxonomy
	    WHEN spc.npi_classification IS NOT NULL    AND  UPPER(SUBSTR(spc.npi_classification,1,1)) <> 'X'   THEN spc.npi_classification
	    ELSE NULL
	END                                                                                     AS clin_obsn_prov_nucc_taxnmy_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  proc_prov_alt_speclty_id and proc_prov_alt_speclty_id_qual
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN prv.derived_specialty IS NOT NULL  AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN prv.derived_specialty
	    WHEN spc.name IS NOT NULL               AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN spc.name
	    ELSE NULL
	END	                                                  							        AS clin_obsn_prov_alt_speclty_id,

	CASE
	    WHEN prv.derived_specialty IS NOT NULL AND UPPER(SUBSTR(prv.derived_specialty,1,1)) <> 'X' THEN 'PROVIDER_DERIVED_SPECIALTY'
	    WHEN spc.name IS NOT NULL              AND UPPER(SUBSTR(spc.name,1,1)) <> 'X'              THEN 'SPECIALTY_NAME'
	    ELSE NULL
	END			    																		AS clin_obsn_prov_alt_speclty_id_qual,
    -------------------------------------------------------------------------------------------------------------------------
    --  clin_obsn_prov_state_cd
    -------------------------------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE(
	        CASE
	            WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2)
	        END
    	)																					AS clin_obsn_prov_state_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  clin_obsn_prov_zip_cd
    -------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 THEN '000'
        ELSE SUBSTR(prc.zip, 1, 3)
    END																					    AS clin_obsn_prov_zip_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_onset_dt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_resltn_dt,
	'PATIENT_SMOKE_HISTORY'                                                                 AS clin_obsn_typ_cd,
	CAST(NULL AS STRING)                                                                    AS clin_obsn_ndc,
	CAST(NULL AS STRING)                                                                    AS clin_obsn_diag_nm,
	smk.description                                                                         AS clin_obsn_msrmt,
	CAST(NULL AS STRING) AS clin_obsn_uom,
    -------------------------------------------------------------------------------------------------------------------------
    --  data_captr_dt
    -------------------------------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(psm.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
	'patient_smoke'																		    AS prmy_src_tbl_nm,
	'136'																			        AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(COALESCE(psm.effective_date, SUBSTR(trs.dos, 1, 10)), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(psm.effective_date, SUBSTR(trs.dos, 1, 10)), 1, 7)
	END                                                                                     AS part_mth
FROM  patient_smoke psm

LEFT OUTER JOIN smoke smk      ON COALESCE(psm.smoke_id, 'NULL') = COALESCE(smk.smoke_id, 'empty')
LEFT OUTER JOIN patient ptn    ON COALESCE(psm.patient_id, 'NULL') = COALESCE(ptn.patient_id, 'empty')
LEFT OUTER JOIN transcript trs ON COALESCE(psm.patient_id, 'NULL') = COALESCE(trs.patient_id, 'empty') AND psm.last_modified = SUBSTR(trs.dos, 1, 10)

LEFT OUTER JOIN provider prv   ON COALESCE(trs.provider_id, 'NULL') = COALESCE(prv.provider_id, 'empty')
LEFT OUTER JOIN practice prc   ON COALESCE(prv.practice_id, 'NULL') = COALESCE(prc.practice_id, 'empty')
LEFT OUTER JOIN specialty spc  ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
LEFT OUTER JOIN matching_payload pay    ON LOWER(COALESCE(ptn.patient_id, 'NULL'))    =  LOWER(COALESCE(pay.claimid, 'empty'))


WHERE TRIM(UPPER(COALESCE(psm.patient_smoke_id, 'empty'))) <> 'PATIENT_SMOKE_ID'
