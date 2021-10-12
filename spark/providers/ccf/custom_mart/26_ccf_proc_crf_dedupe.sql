SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(proc_crf.input_file_name, '/')[SIZE(SPLIT(proc_crf.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  11  
                                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  9
                                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END
                        ),
    	        CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
    	    )																					
	    AS INT)                                                                AS ptnt_birth_yr,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_age_num
    ----------------------------------------------------------------------------------------------------------------------
    CAP_AGE(
        VALIDATE_AGE
            (
                CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  11  
                                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  9
                                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END
                        ),
    	        CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
            )
          )                                                                  AS ptnt_age_num,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_gender_cd
    ----------------------------------------------------------------------------------------------------------------------
    CASE
    	WHEN pay.gender IS NULL AND mptnt.gender IS NULL               THEN NULL
    	WHEN SUBSTR(UPPER(pay.gender ),   1, 1) IN ('F', 'M', 'U')       THEN SUBSTR(UPPER(pay.gender ), 1, 1)
    	WHEN SUBSTR(UPPER(mptnt.gender ), 1, 1) IN ('F', 'M', 'U')     THEN SUBSTR(UPPER(mptnt.gender ), 1, 1)
        ELSE 'U' 
    END                                                                    AS ptnt_gender_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_state_cd
    ----------------------------------------------------------------------------------------------------------------------
    VALIDATE_STATE_CODE(UPPER(pay.state))	                               AS ptnt_state_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_zip3_cd
    ----------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE(pay.threedigitzip)                                       AS ptnt_zip3_cd,
    ----------------------------------------------------------------------------------------------------------------------
	proc_crf.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	proc_crf.deidentified_patient_id                                        AS deidentified_patient_id,
    proc_crf.data_source                                                    AS data_source,
    proc_crf.proc_id                                                        AS proc_id,
    proc_crf.visit_encounter_id                                             AS visit_encounter_id,

    proc_crf.proc_concept_name                                              AS proc_concept_name,
    proc_crf.proc_concept_code                                              AS proc_concept_code,
    proc_crf.proc_system_name                                               AS proc_system_name,

    proc_crf.src_proc_concept_code                                          AS src_proc_concept_code,
    proc_crf.src_proc_system_name                                           AS src_proc_system_name,
    proc_crf.src_proc_concept_name                                          AS src_proc_concept_name,

    proc_crf.proc_status_concept_name                                       AS proc_status_concept_name,
    proc_crf.proc_status_concept_code                                       AS proc_status_concept_code,
    ----------------------------------------------------------------------------------------------------------------------
    --   proc_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  11  
                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  9
                 THEN TO_DATE(proc_crf.proc_start_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(proc_crf.proc_start_date ,'')) =  4 
                 THEN proc_crf.proc_start_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                   AS proc_start_date,
    ----------------------------------------------------------------------------------------------------------------------
    --   proc_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  11  
                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  9
                 THEN TO_DATE(proc_crf.proc_end_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(proc_crf.proc_end_date ,'')) =  4 
                 THEN proc_crf.proc_end_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                   AS proc_end_date,
    ----------------------------------------------------------------------------------------------------------------------
    proc_crf.procedure_domain                                               AS procedure_domain,
    proc_crf.physician_notes_proc_avail                                     AS physician_notes_proc_avail,
    proc_crf.category                                                       AS category,
    ----------------------------------------------------------------------------------------------------------------------
    --   date_of_event
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(proc_crf.date_of_event ,'')) =  11  
                 THEN TO_DATE(proc_crf.date_of_event ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(proc_crf.date_of_event ,'')) =  9
                 THEN TO_DATE(proc_crf.date_of_event ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(proc_crf.date_of_event ,'')) =  4 
                 THEN proc_crf.date_of_event 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                   AS date_of_event,
    ----------------------------------------------------------------------------------------------------------------------
    proc_crf.event_type                                                     AS event_type,
    proc_crf.indication                                                     AS indication,
    
    proc_crf.max_extent_active_disease                                      AS max_extent_active_disease,
    proc_crf.max_extent_of_exam                                             AS max_extent_of_exam,
    proc_crf.modified_mayo_endoscopy_score                                  AS modified_mayo_endoscopy_score,
    
    proc_crf.sum_of_mayo_endo_subscore                                      AS sum_of_mayo_endo_subscore,
    proc_crf.diagnosis                                                      AS diagnosis,
    proc_crf.location                                                       AS location,
    proc_crf.size_of_ulcers                                                 AS size_of_ulcers,
    
    proc_crf.affected_surface                                               AS affected_surface,
    proc_crf.ulcerated_surface                                              AS ulcerated_surface,

    proc_crf.presence_of_narrowings                                         AS presence_of_narrowings,
    proc_crf.mayo_endoscopic_subscore                                       AS mayo_endoscopic_subscore,
    proc_crf.visualized                                                     AS visualized,
    proc_crf.erythema                                                       AS erythema,
    proc_crf.vascular_pattern                                               AS vascular_pattern,
    proc_crf.friability                                                     AS friability,
    proc_crf.mucosal_breaks                                                 AS mucosal_breaks,
    proc_crf.ses_cd_subscore                                                AS ses_cd_subscore,
    proc_crf.procedure_results                                              AS procedure_results,
    proc_crf.erosions                                                       AS erosions,

    proc_crf.stricture                                                      AS stricture,
    proc_crf.stricture_severity                                             AS stricture_severity,
    proc_crf.rutgeerts_score                                                AS rutgeerts_score,
    proc_crf.pathology                                                      AS pathology,
    'procedures_crf'                                                            AS prmy_src_tbl_nm,
    'ccf'                                                                       AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  procedures_crf proc_crf
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(proc_crf.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE proc_crf.proc_id <> 'PROC_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36, 37, 38, 39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54
--limit 10
