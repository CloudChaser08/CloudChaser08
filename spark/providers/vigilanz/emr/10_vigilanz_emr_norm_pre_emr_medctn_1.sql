SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_medctn_id
    --------------------------------------------------------------------------------------------------
    CONCAT  ('250', '_', madm.client_id, 
            '_', madm.encounter_id, '_', madm.drug_name, 
            '_', madm.administration_date_time)                                             AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'09'                                                                                    AS mdl_vrsn_num,
    SPLIT(madm.input_file_name, '/')[SIZE(SPLIT(madm.input_file_name, '/')) - 1]            AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    COALESCE(madm.client_id, med.client_id)                                                 AS vdr_org_id,
    CONCAT  (madm.client_id, '_', madm.encounter_id, 
            '_', madm.drug_name, '_', madm.administration_date_time)                        AS vdr_medctn_admin_id,
    'MEDICATION_ADMINISTRATION_ID'                                                          AS vdr_medctn_admin_id_qual,        
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
--    COALESCE(pay.hvid, CONCAT('250_', pay.claimid))											AS hvid,
    pay.hvid                                                                                AS hvid,
/*
    CASE 
        WHEN (YEAR(COALESCE(pay.yearofbirth, '{VDR_FILE_DT}', '1900-01-01')) - pay.yearofbirth) > 84 THEN 1927 
        ELSE pay.yearofbirth 
    END                                                                                     AS ptnt_birth_yr,
*/

	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(enc.admission_date, '%Y%m%d') AS DATE), pay.yearofbirth),
            CAST(EXTRACT_DATE(enc.admission_date, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                    AS ptnt_birth_yr,

    CASE 
        WHEN pay.gender IS NULL THEN NULL
        WHEN pay.gender IN ('F', 'M', 'U') THEN pay.gender 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,

    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	CAST(MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3)) AS STRING)                          AS ptnt_zip3_cd,
    --------------------------------------------------------------------------------------------------
    --  enc_start_dt - Add Capping when date is available - Use CASE Logic for the dates 
    --------------------------------------------------------------------------------------------------
    CONCAT('250_', enc.encounter_id)                                                        AS hv_enc_id,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(enc.admission_date, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(enc.admission_date, 1, 19)                                              
    END                                                                                     AS enc_dt,
    
	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(madm.administration_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(madm.administration_date_time, 1, 19)                                              
    END                                                                                     AS medctn_admin_dt,    

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(med.start_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(med.start_date_time, 1, 19)                                              
    END                                                                                     AS medctn_start_dt,    

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(med.stop_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(med.stop_date_time, 1, 19)                                              
    END                                                                                     AS medctn_end_dt,    

	UPPER(madm.status)                                                                      AS medctn_ord_stat_cd,
	
	CASE
	    WHEN madm.status IS NOT NULL THEN 'ADMINISTRATION STATUS'
	    ELSE NULL
    END                                                                                     AS medctn_ord_stat_cd_qual,
    
    CLEAN_UP_NDC_CODE
    (
        COALESCE(madm.ndc_code, med.ndc_code)  
        
    )                                                                                       AS medctn_ndc,
    COALESCE(madm.drug_name, med.drug_name)                                                 AS medctn_genc_nm,	    
	COALESCE(madm.dose, med.dose)                                                           AS medctn_dose_txt,  
	    
	CASE
	    WHEN COALESCE(madm.dose, med.dose) IS NOT NULL THEN 'MEDICATION DOSE'
        ELSE NULL
    END                                                                                     AS medctn_dose_txt_qual,
	    
	COALESCE(madm.dose_unit, med.dose_unit)                                                 AS medctn_dose_uom,
	COALESCE(madm.route, med.route)                                                         AS medctn_admin_rte_txt,
    'medication administration'                                                             AS prmy_src_tbl_nm,
    '250'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.admission_date,1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admission_date, 1, 7)
	END																					    AS part_mth

FROM    medication_administration madm
LEFT OUTER JOIN medication med 
            ON  COALESCE(madm.encounter_id, 'NULL') = COALESCE(med.encounter_id, 'empty')
            AND COALESCE(madm.client_id, 'NULL')    = COALESCE(med.client_id, 'empty') 
LEFT OUTER JOIN encounter enc
            ON  COALESCE(madm.encounter_id, 'NULL') = COALESCE(enc.encounter_id, 'empty')
            AND COALESCE(madm.client_id, 'NULL')    = COALESCE(enc.client_id, 'empty')
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL')        = COALESCE(pay.claimid, 'empty')
WHERE UPPER(madm.row_id) <> 'ROWID'
