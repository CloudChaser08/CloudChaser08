SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', haq.encounter_id)														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(haq.input_file_name, '/')[SIZE(SPLIT(haq.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CAST(haq.practice_id AS STRING)                                                         AS vdr_org_id,
	haq.encounter_id												        				AS vdr_clin_obsn_id,
	CASE 
	    WHEN haq.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID' 
	    ELSE NULL 
	END																		                AS vdr_clin_obsn_id_qual,
 
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN pay.hvid           IS NOT NULL THEN pay.hvid
        WHEN pay.personid       IS NOT NULL THEN CONCAT('241_' , pay.personid)
        ELSE NULL 
    END																			            AS hvid,
    COALESCE(pln.date_of_birth, pay.yearofbirth)                                            AS ptnt_birth_yr,
    CASE 
        WHEN pln.sex IN ('F', 'M', 'U', 'null') THEN pln.sex 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', haq.encounter_id)                                                        AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS clin_obsn_prov_npi,
    
    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS clin_obsn_prov_qual,
    enc.encounter_rendering_provider_id                                                     AS clin_obsn_prov_vdr_id,
    
    CASE
        WHEN enc.encounter_rendering_provider_id IS NULL THEN NULL
        ELSE 'RENDERING_PROVIDER_ID'
    END                                                                                     AS clin_obsn_prov_vdr_id_qual,
    
    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS clin_obsn_prov_mdcr_speclty_cd,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN COALESCE(prov.provider_type, prov.provider_local_specialty_name)
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name      IN ('Provider local specialty name', 'null')
            THEN prov.provider_type    
        WHEN prov.provider_type                         IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN prov.provider_local_specialty_name
        ELSE NULL
    END                                                                                     AS clin_obsn_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', 'null')
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS clin_obsn_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS clin_obsn_prov_zip_cd,    
    CAST(NULL AS STRING)                                                                    AS clin_obsn_onset_dt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_resltn_dt,
    'HAQ-II Score'                                                                          AS clin_obsn_typ_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,
    haq.haq_ii_score                                                                        AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_grp_txt,
    haq.enterprise_id                                                                       AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(haq.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'haq_ii_score'                                                                          AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM    haq_ii_score haq
LEFT OUTER JOIN encounters enc 
                ON enc.encounter_id = haq.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = haq.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey
WHERE haq.patient_id <> 'PatientID'
-- LIMIT 10
