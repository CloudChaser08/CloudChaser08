SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', mdhaq.encounter_id)														AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(mdhaq.input_file_name, '/')[SIZE(SPLIT(mdhaq.input_file_name, '/')) - 1]          AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CAST(mdhaq.practice_id AS STRING)                                                       AS vdr_org_id,
	mdhaq.encounter_id												        				AS vdr_clin_obsn_id,
	CASE 
	    WHEN mdhaq.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID' 
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
        ELSE 'U'
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', mdhaq.encounter_id)                                                      AS hv_enc_id,
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

    CASE
        WHEN pat_resp_explode.n = 0 THEN 
            CASE 
                WHEN mdhaq.haq_disability_index IS NOT NULL THEN 'haq_disability_index'
                ELSE NULL
            END
            
        WHEN pat_resp_explode.n = 1 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_score IS NOT NULL THEN 'patient_reported_pain_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 2 THEN 
            CASE 
                WHEN mdhaq.patient_reported_global_score IS NOT NULL THEN 'patient_reported_global_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 3 THEN 
            CASE 
                WHEN mdhaq.physican_global_score IS NOT NULL THEN 'physican_global_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 4 THEN 
            CASE 
                WHEN mdhaq.rapid_3_score IS NOT NULL THEN 'rapid_3_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 5 THEN 
            CASE 
                WHEN mdhaq.rapid_3_score_raw_score IS NOT NULL THEN 'rapid_3_score_raw_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 6 THEN 
            CASE 
                WHEN mdhaq.rapid_4_raw_score IS NOT NULL THEN 'rapid_4_raw_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 7 THEN 
            CASE 
                WHEN mdhaq.rapid_4_score IS NOT NULL THEN 'rapid_4_score'
                ELSE NULL
            END 

        WHEN pat_resp_explode.n = 8 THEN 
            CASE
                WHEN mdhaq.rapid_5_raw_score IS NOT NULL THEN 'rapid_5_raw_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 9 THEN 
            CASE 
                WHEN mdhaq.rapid_5_score IS NOT NULL THEN 'rapid_5_score'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 10 THEN 
            CASE 
                WHEN mdhaq.patient_reported_back_pain IS NOT NULL THEN 'patient_reported_back_pain'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 11 THEN 
            CASE 
                WHEN mdhaq.patient_reported_neck_pain IS NOT NULL THEN 'patient_reported_neck_pain'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 12 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_ankle IS NOT NULL THEN 'patient_reported_pain_left_ankle'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 13 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_elbow IS NOT NULL THEN 'patient_reported_pain_left_elbow'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 14 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_fingers IS NOT NULL THEN 'patient_reported_pain_left_fingers'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 15 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_hip IS NOT NULL THEN 'patient_reported_pain_left_hip'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 16 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_knee IS NOT NULL THEN 'patient_reported_pain_left_knee'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 17 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_shoulder IS NOT NULL THEN 'patient_reported_pain_left_shoulder'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 18 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_toes IS NOT NULL THEN 'patient_reported_pain_left_toes'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 19 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_left_wrist IS NOT NULL THEN 'patient_reported_pain_left_wrist'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 20 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_ankle IS NOT NULL THEN 'patient_reported_pain_right_ankle'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 21 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_elbow IS NOT NULL THEN 'patient_reported_pain_right_elbow'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 22 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_fingers IS NOT NULL THEN 'patient_reported_pain_right_fingers'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 23 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_hip IS NOT NULL THEN 'patient_reported_pain_right_hip'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 24 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_knee IS NOT NULL THEN 'patient_reported_pain_right_knee'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 25 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_shoulder IS NOT NULL THEN 'patient_reported_pain_right_shoulder'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 26 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_toes IS NOT NULL THEN 'patient_reported_pain_right_toes'
                ELSE NULL
            END

        WHEN pat_resp_explode.n = 27 THEN 
            CASE 
                WHEN mdhaq.patient_reported_pain_right_wrist IS NOT NULL THEN 'patient_reported_pain_right_wrist'
                ELSE NULL
            END


        ELSE NULL
    END                                                                                     AS clin_obsn_typ_cd,

    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,

    CASE
        WHEN ARRAY  (    
                    mdhaq.haq_disability_index,
                    mdhaq.patient_reported_pain_score,
                    mdhaq.patient_reported_global_score,
                    mdhaq.physican_global_score,
                    mdhaq.rapid_3_score,
                    mdhaq.rapid_3_score_raw_score,
                    mdhaq.rapid_4_raw_score,
                    mdhaq.rapid_4_score,
                    mdhaq.rapid_5_raw_score,
                    mdhaq.rapid_5_score,
                    mdhaq.patient_reported_back_pain,
                    mdhaq.patient_reported_neck_pain,
                    mdhaq.patient_reported_pain_left_ankle,
                    mdhaq.patient_reported_pain_left_elbow,
                    mdhaq.patient_reported_pain_left_fingers,
                    mdhaq.patient_reported_pain_left_hip,
                    mdhaq.patient_reported_pain_left_knee,
                    mdhaq.patient_reported_pain_left_shoulder,
                    mdhaq.patient_reported_pain_left_toes,
                    mdhaq.patient_reported_pain_left_wrist,
                    mdhaq.patient_reported_pain_right_ankle,
                    mdhaq.patient_reported_pain_right_elbow,
                    mdhaq.patient_reported_pain_right_fingers,
                    mdhaq.patient_reported_pain_right_hip,
                    mdhaq.patient_reported_pain_right_knee,
                    mdhaq.patient_reported_pain_right_shoulder,
                    mdhaq.patient_reported_pain_right_toes,
                    mdhaq.patient_reported_pain_right_wrist
                    )[pat_resp_explode.n] IS NULL       THEN NULL
        ELSE ARRAY (    
                    mdhaq.haq_disability_index,
                    mdhaq.patient_reported_pain_score,
                    mdhaq.patient_reported_global_score,
                    mdhaq.physican_global_score,
                    mdhaq.rapid_3_score,
                    mdhaq.rapid_3_score_raw_score,
                    mdhaq.rapid_4_raw_score,
                    mdhaq.rapid_4_score,
                    mdhaq.rapid_5_raw_score,
                    mdhaq.rapid_5_score,
                    mdhaq.patient_reported_back_pain,
                    mdhaq.patient_reported_neck_pain,
                    mdhaq.patient_reported_pain_left_ankle,
                    mdhaq.patient_reported_pain_left_elbow,
                    mdhaq.patient_reported_pain_left_fingers,
                    mdhaq.patient_reported_pain_left_hip,
                    mdhaq.patient_reported_pain_left_knee,
                    mdhaq.patient_reported_pain_left_shoulder,
                    mdhaq.patient_reported_pain_left_toes,
                    mdhaq.patient_reported_pain_left_wrist,
                    mdhaq.patient_reported_pain_right_ankle,
                    mdhaq.patient_reported_pain_right_elbow,
                    mdhaq.patient_reported_pain_right_fingers,
                    mdhaq.patient_reported_pain_right_hip,
                    mdhaq.patient_reported_pain_right_knee,
                    mdhaq.patient_reported_pain_right_shoulder,
                    mdhaq.patient_reported_pain_right_toes,
                    mdhaq.patient_reported_pain_right_wrist
                    )[pat_resp_explode.n]                   
    END                                                                                     AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_grp_txt,
    mdhaq.enterprise_id                                                                     AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(mdhaq.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'mdhaq_patient_responses'                                                               AS prmy_src_tbl_nm,
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

FROM    mdhaq_patient_responses mdhaq
LEFT OUTER JOIN encounters enc 
                ON enc.encounter_id = mdhaq.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = mdhaq.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey

-- Add Cross Join Array here
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 
                                    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27)) AS n) pat_resp_explode
WHERE
----------- Patient response code explosion
    (
        ARRAY
            (
                mdhaq.haq_disability_index,
                mdhaq.patient_reported_pain_score,
                mdhaq.patient_reported_global_score,
                mdhaq.physican_global_score,
                mdhaq.rapid_3_score,
                mdhaq.rapid_3_score_raw_score,
                mdhaq.rapid_4_raw_score,
                mdhaq.rapid_4_score,
                mdhaq.rapid_5_raw_score,
                mdhaq.rapid_5_score,
                mdhaq.patient_reported_back_pain,
                mdhaq.patient_reported_neck_pain,
                mdhaq.patient_reported_pain_left_ankle,
                mdhaq.patient_reported_pain_left_elbow,
                mdhaq.patient_reported_pain_left_fingers,
                mdhaq.patient_reported_pain_left_hip,
                mdhaq.patient_reported_pain_left_knee,
                mdhaq.patient_reported_pain_left_shoulder,
                mdhaq.patient_reported_pain_left_toes,
                mdhaq.patient_reported_pain_left_wrist,
                mdhaq.patient_reported_pain_right_ankle,
                mdhaq.patient_reported_pain_right_elbow,
                mdhaq.patient_reported_pain_right_fingers,
                mdhaq.patient_reported_pain_right_hip,
                mdhaq.patient_reported_pain_right_knee,
                mdhaq.patient_reported_pain_right_shoulder,
                mdhaq.patient_reported_pain_right_toes,
                mdhaq.patient_reported_pain_right_wrist
            )[pat_resp_explode.n] IS NOT NULL
    )
-- Remove header records
    AND TRIM(lower(COALESCE(mdhaq.patient_id, 'empty'))) <> 'patientid'
-- LIMIT 10
