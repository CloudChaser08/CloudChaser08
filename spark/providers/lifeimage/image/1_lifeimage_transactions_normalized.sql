SELECT

    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    CONCAT('157', '_', txn.studyinstanceuid, '_', txn.seriesinstanceuid )                   AS hv_srs_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set_nm,
	'471'                                                                                   AS hvm_vdr_id,
	'157'                                                                                   AS hvm_vdr_feed_id, 
    txn.studyinstanceuid			                                                        AS vdr_study_id,
    txn.seriesinstanceuid			                                                        AS vdr_srs_id,
    pay.hvid                                                                                AS hvid,    
	/* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
        (
            cast(COALESCE(
                        CASE
                            WHEN LOCATE('Y', txn.patientage) <> 0 THEN REPLACE(txn.patientage,'Y','')
                            WHEN LOCATE('M', txn.patientage) <> 0 THEN 1                            
                        ELSE NULL
                        END,
                        pay.age
                    ) AS INT
                  ),
            CAST(EXTRACT_DATE(COALESCE(txn.studydate, '{VDR_FILE_DT}'), '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS ptnt_birth_yr,
	/* ptnt_age_num */
	VALIDATE_AGE
        (
            cast(COALESCE(
                        CASE
                            WHEN LOCATE('Y', txn.patientage) <> 0 THEN REPLACE(txn.patientage,'Y','')
                            WHEN LOCATE('M', txn.patientage) <> 0 THEN 1                            
                        ELSE NULL
                        END,
                        NULL
                    ) AS INT),
            CAST(EXTRACT_DATE(COALESCE(txn.studydate, '{VDR_FILE_DT}'), '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS ptnt_age_num,

	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.patientsex, 'U')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(txn.patientsex, 'U')), 1, 1)
        	    WHEN SUBSTR(UPPER(COALESCE(pay.gender    , 'U')), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(COALESCE(pay.gender    , 'U')), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(pay.state)                                                          AS ptnt_state_cd,
    MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS ptnt_zip3_cd,
    --------------- Weight - apply privacy filter
    -- CLEAN_UP_VITAL_SIGN(sign_type, sign_measurement, sign_units, gender, age, year_of_birth, measurement_date, encounter_date):
    CLEAN_UP_VITAL_SIGN
        (
        -- sign_type, sign_measurement, sign_units
        'WEIGHT' ,convert_value(txn.patientweight,'KILOGRAMS_TO_POUNDS'), 'POUNDS',
        -- Gender
        SUBSTR(UPPER(COALESCE(txn.patientsex,pay.gender,  'U')), 1, 1)  ,
        -- age
        COALESCE
            (
            CASE 
                WHEN LOCATE('Y', txn.patientage) <> 0 THEN REPLACE(txn.patientage,'Y','')
                WHEN LOCATE('M', txn.patientage) <> 0 THEN 1                            
            ELSE NULL
            END,
            pay.age
            ),
       -- Year of Birth  
        pay.yearofbirth, 
        -- measurement_date
        CAST(EXTRACT_DATE(txn.studydate,'%Y-%m-%d') AS DATE),     
        -- encounter_date
        CAST(EXTRACT_DATE(txn.studydate, '%Y-%m-%d') AS DATE)
        )                                                                                   AS  ptnt_weight_msrmt,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.studydate, '%Y-%m-%d') AS DATE), 
            esdt.gen_ref_1_dt,
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS imgg_stdy_dt,
    txn.sopinstanceuid_cnt		                                                            AS srs_img_cnt,    
    txn.referringphysicianname			                                                    AS rfrg_prov_nm,
    txn.requestingphysician			                                                        AS ordg_prov_nm,
    txn.modality			                                                                AS img_mdlty_cd,
    txn.manufacturer			                                                            AS imgg_eqpmt_manfctr_nm,
    txn.manufacturermodelname			                                                    AS imgg_eqpmt_manfctr_mdl_nm,
    txn.bodypartexamined			                                                        AS imgd_bdy_prt_cd,
	'157'                                                                                   AS part_hvm_vdr_feed_id,
    /* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.studydate, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE 
	         CONCAT
	            (
                    SUBSTR(txn.studydate, 1, 4), '-',
                    SUBSTR(txn.studydate, 6, 2), '-01'
                )
	END                                                                                 AS part_mth
FROM lifeimage_transactions_dedup txn
LEFT OUTER JOIN matching_payload pay ON txn.hvjoinkey = pay.hvjoinkey
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 157
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 157
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1
   
