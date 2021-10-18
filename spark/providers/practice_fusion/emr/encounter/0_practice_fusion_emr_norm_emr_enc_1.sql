SELECT
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('136_', trs.transcript_id)														AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(trs.input_file_name, '/')[SIZE(SPLIT(trs.input_file_name, '/')) - 1]              AS data_set_nm,
	439                                                                                     AS hvm_vdr_id,
	136                                                                                     AS hvm_vdr_feed_id,
    ptn.practice_id                                                                         AS vdr_org_id,
	trs.transcript_id																		AS vdr_enc_id,
	CASE
	    WHEN trs.transcript_id IS NOT NULL THEN 'TRANSCRIPT_ID'
	    ELSE NULL
	END																		                AS vdr_enc_id_qual,
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

	        VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(COALESCE(SUBSTR(trs.dos,1, 10), CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE), COALESCE(ptn.birth_year, pay.yearofbirth)),
	        CAST(EXTRACT_DATE(COALESCE(SUBSTR(trs.dos,1, 10), CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE),
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
    --  enc_start_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(trs.dos,1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_start_dt,
    CAST(NULL AS STRING)                                                                    AS enc_end_dt,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_vdr_id and enc_prov_vdr_qual
    --------------------------------------------------------------------------------------------------
	trs.provider_id																			AS enc_prov_vdr_id,
	CASE
	    WHEN trs.provider_id IS NOT NULL THEN 'TRANSCRIPT_PROVIDER_ID'
	    ELSE NULL
	END																		                AS enc_prov_vdr_id_qual,

	CAST(NULL AS STRING)   AS enc_prov_alt_id,
    CAST(NULL AS STRING)   AS enc_prov_alt_id_qual,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_nucc_taxnmy_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN UPPER(SUBSTR(prv.derived_ama_taxonomy,1,1)) = 'X'  THEN NULL
	    WHEN prv.derived_ama_taxonomy IS NOT NULL               THEN prv.derived_ama_taxonomy
	    WHEN UPPER(SUBSTR(spc.npi_classification,1,1)) = 'X'    THEN NULL
	    WHEN spc.npi_classification IS NOT NULL                 THEN spc.npi_classification
	    ELSE NULL
	END																					    AS enc_prov_nucc_taxnmy_cd,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_alt_speclty_id and enc_prov_alt_speclty_id_qual
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN UPPER(SUBSTR(prv.derived_specialty,1,1)) = 'X'  THEN NULL
	    WHEN prv.derived_specialty IS NOT NULL               THEN prv.derived_specialty
	    WHEN UPPER(SUBSTR(spc.name,1,1)) = 'X'               THEN NULL
	    WHEN spc.name IS NOT NULL                            THEN spc.name
	    ELSE NULL
	END	                                                  							        AS enc_prov_alt_speclty_id,

	CASE
	    WHEN UPPER(SUBSTR(prv.derived_specialty,1,1)) = 'X'  THEN NULL
	    WHEN prv.derived_specialty IS NOT NULL               THEN 'PROVIDER_DERIVED_SPECIALTY'
	    WHEN UPPER(SUBSTR(spc.name,1,1)) = 'X'               THEN NULL
	    WHEN spc.name IS NOT NULL                            THEN 'SPECIALTY_NAME'
	    ELSE NULL
	END			    																		AS enc_prov_alt_speclty_id_qual,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_state_cd
    --------------------------------------------------------------------------------------------------
	VALIDATE_STATE_CODE(
	        CASE
	            WHEN LOCATE(' OR ', UPPER(prc.state)) <> 0 THEN NULL
	            ELSE SUBSTR(UPPER(COALESCE(prc.state, '')), 1, 2)
	        END
    	)																					AS enc_prov_state_cd,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_zip_cd
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN LOCATE(' OR ', UPPER(prc.zip)) <> 0 THEN '000'
        ELSE SUBSTR(prc.zip, 1, 3)
    END																					    AS enc_prov_zip_cd,
--     --------------------------------------------------------------------------------------------------
--     --  enc_typ_nm
--     --------------------------------------------------------------------------------------------------
--     UPPER(apt.appointment_type)  AS enc_typ_nm,

    CAST(NULL AS STRING)  AS enc_typ_nm,
   --------------------------------------------------------------------------------------------------
    --- enc_grp_txt
    --------------------------------------------------------------------------------------------------
          CASE
            WHEN COALESCE (ect.name, ect.name ) IS NULL THEN NULL
            ELSE TRIM(
            SUBSTR(
                        CONCAT
                            (
                                CASE
                                    WHEN ect.name  IS NULL  THEN ''
                                    ELSE CONCAT( '| ENCOUNTER_CATEGORY_NAME: ', ect.name )
                                END,
                                CASE
                                    WHEN etp.name  IS NULL THEN ''
                                    ELSE CONCAT(' - ', etp.name)
                                END
                            ), 3
                    ))
            END                                                                             AS enc_grp_txt,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(trs.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,

	'transcript'																			AS prmy_src_tbl_nm,
	'136'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(trs.dos,1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(SUBSTR(trs.dos,1, 10), 1, 7)
	END																					    AS part_mth
FROM transcript trs
LEFT OUTER JOIN patient ptn     ON COALESCE(trs.patient_id, 'NULL')           = COALESCE(ptn.patient_id, 'empty')
LEFT OUTER JOIN matching_payload pay     ON LOWER(COALESCE(ptn.patient_id, 'NULL'))    = COALESCE(pay.claimid, 'empty')
LEFT OUTER JOIN provider prv    ON COALESCE(trs.provider_id, 'NULL')          = COALESCE(prv.provider_id, 'empty')
LEFT OUTER JOIN practice prc    ON COALESCE(prv.practice_id, 'NULL')          = COALESCE(prc.practice_id, 'empty')
LEFT OUTER JOIN specialty spc   ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
LEFT OUTER JOIN encounter enc     ON COALESCE(trs.transcript_id, 'NULL')      = COALESCE(enc.transcript_id, 'empty')
-- LEFT OUTER JOIN encountertype ect ON COALESCE(enc.enctype_id, 'NULL')         = COALESCE(ect.enctype_id, 'empty')
-- LEFT OUTER JOIN encountercategory en_catg ON COALESCE(ect.enccat_id, 'NULL')       = COALESCE(en_catg.enccat_id, 'empty')

LEFT OUTER JOIN enctype    etp ON COALESCE(enc.enctype_id, 'NULL')    = COALESCE(etp.enctype_id, 'empty')
LEFT OUTER JOIN enccat     ect ON COALESCE(etp.enccat_id, 'NULL')     = COALESCE(ect.enccat_id, 'empty')

WHERE TRIM(lower(COALESCE(trs.transcript_id, 'empty'))) <> 'transcript_id'
--LIMIT 10