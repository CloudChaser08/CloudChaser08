SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_diag_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', diag.encounter_id, '_', diag.standard_diagnosis_code)					AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(diag.input_file_name, '/')[SIZE(SPLIT(diag.input_file_name, '/')) - 1]            AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CONCAT(diag.encounter_id, '_', diag.standard_diagnosis_code)							AS vdr_diag_id,
    'DIAGNOSIS_ID'                                                                          AS vdr_diag_id_qual,
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
        WHEN pln.sex IN ('F', 'M', 'U', NULL) THEN pln.sex 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time,1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,

	CAP_DATE
	    (
            COALESCE
                (
                    CAST(EXTRACT_DATE(SUBSTR(diag.diagnosis_date_time,     1, 8 ), '%Y%m%d') AS DATE),
                    CAST(EXTRACT_DATE(SUBSTR(diag.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE)
                ),
                CAST('{EARLIEST_DIAGNOSIS_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)

	    )																					AS diag_dt,

    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS diag_prov_npi,
    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS diag_prov_qual,
    
    enc.encounter_rendering_provider_id                                                     AS diag_prov_vdr_id,
    CASE
        WHEN enc.encounter_rendering_provider_id IS NOT NULL THEN 'RENDERING_PROVIDER'
        ELSE NULL
    END                                                                                     AS diag_prov_vdr_id_qual,

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS diag_prov_mdcr_speclty_cd,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', NULL)
            THEN COALESCE(prov.provider_type, prov.provider_local_specialty_name)
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name      IN ('Provider local specialty name', NULL)
            THEN prov.provider_type    
        WHEN prov.provider_type                         IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', NULL)
            THEN prov.provider_local_specialty_name
        ELSE NULL
    END                                                                                     AS diag_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', NULL)
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS diag_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS diag_prov_zip_cd,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(diag.diagnosis_symptom_onset_date, 1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_onset_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(diag.diagnosis_resolution_date_time, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_resltn_dt,

    CLEAN_UP_DIAGNOSIS_CODE
        (
            diag.standard_diagnosis_code,
            diag.standard_diagnosis_code_type,
            CAST
                (
                    EXTRACT_DATE(SUBSTR(diag.diagnosis_date_time,1, 8), 
                    '%Y%m%d') AS DATE
                )
        )                                                                                   AS diag_cd,

    CASE
        WHEN diag.standard_diagnosis_code_type = 'ICD9'  THEN '01'
        WHEN diag.standard_diagnosis_code_type = 'ICD10' THEN '02'
        ELSE NULL
    END                                                                                     AS diag_cd_qual,
    
    CONCAT
        (
            'CHRONIC: ', 
                CASE
                    WHEN diag.chronic_y_n IN ('Y', 'N') THEN diag.chronic_y_n
                    ELSE NULL
                END
        )                                                                                   AS diag_grp_txt,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(diag.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'diagnosis'                                                                             AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time,       1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM    diagnosis diag
LEFT OUTER JOIN encounters enc
                ON     enc.encounter_id = diag.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = diag.patient_id
LEFT OUTER JOIN matching_payload pay
                        ON pay.hvjoinkey = pln.hvjoinkey
-- Remove header records
WHERE diag.patient_id <> 'PatientID'
-- LIMIT 10
