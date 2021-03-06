SELECT
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT
        (
            '136_',
            COALESCE(apt.appointment_id, 'NO_APPOINTMENT_ID'), '_',
            COALESCE(trs.transcript_id, 'NO_TRANSCRIPT_ID'  )
        )                                                                                   AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(apt.input_file_name, '/')[SIZE(SPLIT(apt.input_file_name, '/')) - 1]              AS data_set_nm,
	439                                                                                     AS hvm_vdr_id,
	136                                                                                     AS hvm_vdr_feed_id,
    ptn.practice_id                                                                         AS vdr_org_id,
	apt.appointment_id																		AS vdr_enc_id,
	CASE
	    WHEN apt.appointment_id IS NOT NULL THEN 'APPOINTMENT_ID'
	    ELSE NULL
	END																		                AS vdr_enc_id_qual,
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN pay.hvid       IS NOT NULL THEN pay.hvid
        WHEN apt.patient_id IS NOT NULL THEN CONCAT('136_' , apt.patient_id)
        ELSE NULL
    END																			            AS hvid,
    --------------------------------------------------------------------------------------------------
    --  ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH
	    (

	        VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(COALESCE(apt.start_time, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE), COALESCE(ptn.birth_year, pay.yearofbirth)),
	        CAST(EXTRACT_DATE(COALESCE(apt.start_time, CAST('{VDR_FILE_DT}' AS DATE)), '%Y-%m-%d') AS DATE),
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
            CAST(EXTRACT_DATE(apt.start_time, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_start_dt,

    --------------------------------------------------------------------------------------------------
    --  enc_end_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
          CAST(EXTRACT_DATE(apt.end_time, '%Y-%m-%d') AS DATE),
          CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
          CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_end_dt,
    --------------------------------------------------------------------------------------------------
    --  enc_prov_vdr_id and enc_prov_vdr_qual
    --------------------------------------------------------------------------------------------------
	apt.provider_id																			AS enc_prov_vdr_id,
	CASE
	    WHEN apt.provider_id IS NOT NULL THEN 'APPOINTMENT_PROVIDER_ID'
	    ELSE NULL
	END																		                AS enc_prov_vdr_id_qual,
	trs.provider_id																			AS enc_prov_alt_id,
	CASE
	    WHEN trs.provider_id IS NOT NULL THEN 'TRANSCRIPT_PROVIDER_ID'
	    ELSE NULL
	END																		                AS enc_prov_alt_id_qual,
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
    --------------------------------------------------------------------------------------------------
    --  enc_typ_nm  CASE added after discussion with Austin 2021-05-10
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN ref.gen_ref_nm  In ('HOME VISIT', 'PRE OP VISIT', 'DENTAL VISIT') THEN NULL
        ELSE ref.gen_ref_nm
    END                                                                                     AS enc_typ_nm,
    CAST(NULL AS STRING)                                                                    AS enc_grp_txt,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(trs.last_modified, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
	'appointment'																			AS prmy_src_tbl_nm,
	'136'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(COALESCE(apt.start_time, SUBSTR(trs.dos,1, 10)), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(apt.start_time, SUBSTR(trs.dos,1, 10)), 1, 7)
	END																					    AS part_mth
FROM appointment apt
LEFT OUTER JOIN transcript trs  ON COALESCE(apt.transcript_id, 'NULL')     = COALESCE(trs.transcript_id, 'empty')
LEFT OUTER JOIN patient ptn     ON COALESCE(apt.patient_id, 'NULL')        = COALESCE(ptn.patient_id, 'empty')
LEFT OUTER JOIN matching_payload pay     ON LOWER(COALESCE(ptn.patient_id, 'NULL')) = COALESCE(pay.claimid, 'empty')
LEFT OUTER JOIN provider prv    ON COALESCE(apt.provider_id, 'NULL')       = COALESCE(prv.provider_id, 'empty')
LEFT OUTER JOIN practice prc    ON COALESCE(prv.practice_id, 'NULL')       = COALESCE(prc.practice_id, 'empty')
LEFT OUTER JOIN specialty spc   ON COALESCE(prv.primary_specialty_id, 'NULL') = COALESCE(spc.specialty_id, 'empty')
LEFT OUTER JOIN gen_ref_whtlst ref ON UPPER(apt.appointment_type) = UPPER(ref.gen_ref_nm) AND gen_ref_domn_nm = 'emr_enc.enc_typ_nm' AND gen_ref_whtlst_flg ='Y'

WHERE TRIM(lower(COALESCE(apt.appointment_id, 'empty'))) <> 'appointment_id'
