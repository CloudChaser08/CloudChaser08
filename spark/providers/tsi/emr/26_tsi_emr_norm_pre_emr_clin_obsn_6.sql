SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', vit.unique_id)   														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(vit.input_file_name, '/')[SIZE(SPLIT(vit.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	vit.practice_id                                                                         AS vdr_org_id,
	vit.unique_id	    											        				AS vdr_clin_obsn_id,
	CASE 
	    WHEN vit.unique_id IS NOT NULL THEN 'VITAL_SIGN_ID' 
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
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex 
        ELSE NULL
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', vit.encounter_id)                                                        AS hv_enc_id,
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

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vit.date_vital_signs_taken, 1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_onset_dt,

	CAST(NULL AS STRING)                                                                    AS clin_obsn_resltn_dt,

   CASE
        WHEN clin_obsn_typ_cd_explode.n = 0     THEN 'weight_in_pounds'
        WHEN clin_obsn_typ_cd_explode.n = 1     THEN 'pulse_rate'
        WHEN clin_obsn_typ_cd_explode.n = 2     THEN 'diastolic_blood_pressure'
        WHEN clin_obsn_typ_cd_explode.n = 3     THEN 'systolic_blood_pressure'
        ELSE NULL
    END                                                                                     AS clin_obsn_typ_cd,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,

    CASE
        WHEN ARRAY  (    
                    vit.weight_in_pounds,
                    vit.pulse_rate,
                    vit.diastolic_blood_pressure,
                    vit.systolic_blood_pressure
                    )[clin_obsn_typ_cd_explode.n] IS NULL       THEN NULL
        ELSE ARRAY (    
                    vit.weight_in_pounds,
                    vit.pulse_rate,
                    vit.diastolic_blood_pressure,
                    vit.systolic_blood_pressure
                    )[clin_obsn_typ_cd_explode.n]                   
    END                                                                                     AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_grp_txt,
    vit.enterprise_id                                                                       AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vit.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'vital_sign'                                                                            AS prmy_src_tbl_nm,
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


FROM    vital_signs vit
LEFT OUTER JOIN encounters enc       ON enc.encounter_id = vit.encounter_id
LEFT OUTER JOIN provider prov        ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln         ON pln.person_id = vit.patient_id
LEFT OUTER JOIN matching_payload pay ON pay.hvjoinkey = pln.hvjoinkey

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS n) clin_obsn_typ_cd_explode
WHERE
----------- clin_obsn_typ_cd code explosion
    (
        ARRAY
            (
                vit.weight_in_pounds,
                vit.pulse_rate,
                vit.diastolic_blood_pressure,
                vit.systolic_blood_pressure
            )[clin_obsn_typ_cd_explode.n] IS NOT NULL
    )
-- Remove header records
    AND TRIM(lower(COALESCE(vit.patient_id, 'empty'))) <> 'patientid' 
--------------------------------------------------------------------------------------------------
--  UNION ALL with vital for height
--------------------------------------------------------------------------------------------------
UNION ALL

SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', vit.unique_id)   														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(vit.input_file_name, '/')[SIZE(SPLIT(vit.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	vit.practice_id                                                                         AS vdr_org_id,
	vit.unique_id	    											        				AS vdr_clin_obsn_id,
	CASE 
	    WHEN vit.unique_id IS NOT NULL THEN 'VITAL_SIGN_ID' 
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
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex 
        ELSE NULL
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', vit.encounter_id)                                                        AS hv_enc_id,
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

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vit.date_vital_signs_taken, 1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_onset_dt,

	CAST(NULL AS STRING)                                                                    AS clin_obsn_resltn_dt,

    CASE 
        WHEN CAST(COALESCE(vit.height_in_total_centimeters, vit.height_in_total_inches, 
                vit.height_in_centimeters,vit.height_in_feet,vit.height_in_inches, 0) AS FLOAT) <> 0 THEN 'HEIGHT'
    ELSE
    NULL
    END                                                                                     AS clin_obsn_typ_cd,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,


    CASE
        WHEN vit.height_in_total_centimeters IS NOT NULL AND CAST(vit.height_in_total_centimeters AS FLOAT) <> 0 
                        THEN CONVERT_CM_TO_IN(vit.height_in_total_centimeters)
        WHEN vit.height_in_total_inches      IS NOT NULL AND CAST(vit.height_in_total_inches      AS FLOAT) <> 0 
                        THEN vit.height_in_total_inches
        WHEN vit.height_in_centimeters       IS NOT NULL AND CAST(vit.height_in_centimeters       AS FLOAT) <> 0 
                        THEN CONVERT_CM_TO_IN(vit.height_in_centimeters)
        WHEN vit.height_in_feet              IS NOT NULL AND CAST(vit.height_in_feet              AS FLOAT) <> 0 
                        THEN (vit.height_in_feet*12) + COALESCE(vit.height_in_inches, 0)
    ELSE
    NULL
    END                                                                                     AS clin_obsn_msrmt,
    CASE 
        WHEN CAST(COALESCE(vit.height_in_total_centimeters, vit.height_in_total_inches, 
                vit.height_in_centimeters,vit.height_in_feet,vit.height_in_inches, 0) AS FLOAT) <> 0 THEN 'INCHES'
    ELSE
    NULL
    END                                                                                     AS clin_obsn_uom,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_grp_txt,
    vit.enterprise_id                                                                       AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vit.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'vital_sign'                                                                            AS prmy_src_tbl_nm,
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


FROM    vital_signs vit
LEFT OUTER JOIN encounters enc       ON enc.encounter_id = vit.encounter_id
LEFT OUTER JOIN provider prov        ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln         ON pln.person_id = vit.patient_id
LEFT OUTER JOIN matching_payload pay ON pay.hvjoinkey = pln.hvjoinkey
WHERE  CAST(COALESCE(vit.height_in_total_centimeters, vit.height_in_total_inches, 
                vit.height_in_centimeters,vit.height_in_feet,vit.height_in_inches, 0) AS FLOAT) <> 0
 AND TRIM(lower(COALESCE(vit.patient_id, 'empty'))) <> 'patientid'
