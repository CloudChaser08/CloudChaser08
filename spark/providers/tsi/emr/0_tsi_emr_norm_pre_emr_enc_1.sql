SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', enc.encounter_id)														AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	enc.encounter_id																		AS vdr_enc_id,
	CASE 
	    WHEN enc.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID' 
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
        ELSE NULL 
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
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_start_dt,
    CAST(NULL AS STRING)                                                                    AS enc_end_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS enc_prov_npi,

    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS enc_prov_qual,

    CASE
        WHEN ARRAY  (    
                    enc.encounter_billing_provider_id,
                    enc.encounter_rendering_provider_id
                    )[prov_explode.n] IS NULL       THEN NULL
        ELSE ARRAY (    
                    enc.encounter_billing_provider_id,
                    enc.encounter_rendering_provider_id
                    )[prov_explode.n]                   
    END                                                                                     AS enc_prov_vdr_id,

    CASE
        WHEN prov_explode.n = 0     THEN 'BILLING_PROVIDER'
        WHEN prov_explode.n = 1     THEN 'RENDERING_PROVIDER'
        ELSE NULL
    END                                                                                     AS enc_prov_vdr_id_qual,

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS enc_prov_mdcr_speclty_cd,

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
    END                                                                                     AS enc_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', 'null')
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS enc_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS enc_prov_zip_cd,
    UPPER(enc.encounter_type)                                                               AS enc_typ_nm,
    CAST(NULL AS STRING)                                                                    AS enc_stat_cd,
    CAST(NULL AS STRING)                                                                    AS enc_stat_cd_qual,
    CONCAT('ENCOUNTER_FACILITY_ID: ', enc.encounter_facility)                               AS enc_grp_txt,

    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR (enc.create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'encounters'                                                                            AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time,1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM    encounters enc
LEFT OUTER JOIN provider prov
            ON prov.provider_id = COALESCE(enc.encounter_rendering_provider_id, 0) <> 0
LEFT OUTER JOIN plainout pln
            ON pln.person_id = enc.patient_id
LEFT OUTER JOIN matching_payload pay
            ON pay.hvjoinkey = pln.hvjoinkey
            
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) prov_explode
WHERE
----------- Provider code explosion
    (
        ARRAY
            (
                enc.encounter_billing_provider_id,
                enc.encounter_rendering_provider_id
            )[prov_explode.n] IS NOT NULL
    )
-- Remove header records
    AND TRIM(lower(COALESCE(enc.patient_id, 'empty'))) <> 'patientid'
