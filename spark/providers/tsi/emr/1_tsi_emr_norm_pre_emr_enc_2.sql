SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', apt.appointment_id)														AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(apt.input_file_name, '/')[SIZE(SPLIT(apt.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	apt.appointment_id																		AS vdr_enc_id,
	CASE 
	    WHEN apt.appointment_id IS NOT NULL THEN 'APPOINTMENT_ID' 
	    ELSE NULL 
	END																		                AS vdr_enc_id_qual,
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
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    --------------------------------------------------------------------------------------------------
    --  enc_start_dt - Add Capping when date is available - Use CASE Logic for the dates 
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(apt.appointment_date_time,1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_start_dt,
    CAST(NULL AS STRING)                                                                    AS enc_end_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS enc_prov_npi,
    
    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS enc_prov_qual,
    
    apt.appointment_provider_id                                                             AS enc_prov_vdr_id,
    CASE
        WHEN apt.appointment_provider_id IS NOT NULL THEN 'RENDERING_PROVIDER'
        ELSE NULL
    END                                                                                     AS enc_prov_vdr_id_qual,

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS enc_prov_mdcr_speclty_cd,

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
    END                                                                                     AS enc_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', NULL)
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS enc_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS enc_prov_zip_cd,
    apt.event_name                                                                          AS enc_typ_nm,
    apt.appointment_status                                                                  AS enc_stat_cd,
    'APPOINTMENT_STATUS'                                                                    AS enc_stat_cd_qual,
    CONCAT('APPOINTMENT_LOCATION: ', apt.appointment_location)                              AS enc_grp_txt,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR ( apt.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'appointments'                                                                          AS prmy_src_tbl_nm,
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

FROM    appointments apt
LEFT OUTER JOIN encounters enc
                ON COALESCE(apt.encounter_id, 'NULL')   = COALESCE(enc.encounter_id, 'empty')
LEFT OUTER JOIN provider prov
                ON prov.provider_id = COALESCE(apt.appointment_provider_id, 0) <> 0
LEFT OUTER JOIN plainout pln
                ON pln.person_id = apt.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey
-- Remove header records
WHERE apt.patient_id <> 'PatientID'
-- LIMIT 10
