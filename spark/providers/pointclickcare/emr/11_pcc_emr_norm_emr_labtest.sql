SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_lab_test_id
    --------------------------------------------------------------------------------------------------
    CONCAT('156', '_', lts.factlabtestid)                                                   AS hv_lab_test_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'03'                                                                                    AS mdl_vrsn_num,
    SPLIT(lts.input_file_name, '/')[SIZE(SPLIT(lts.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_lab_test_id
    --------------------------------------------------------------------------------------------------
    lts.labtestid	                                                                        AS vdr_lab_test_id,
	CASE
	    WHEN lts.labtestid IS NOT NULL THEN 'LAB_TEST_ID'
    ELSE NULL
	END                                                                                     AS vdr_lab_test_id_qual,
   ---------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))        THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(lts.residentid, '')))  THEN CONCAT('156_', lts.residentid)
	ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
    CAST(
        CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(lts.reporteddateid, '%Y%m%d') AS DATE),
	        pay.yearofbirth
	    )
	  AS INT)                                                                               AS ptnt_birth_yr,
    --------------------------------------------------------------------------------------------------
    --- ptnt_gender_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				    	AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(pay.state))													AS ptnt_state_cd,
	MASK_ZIP_CODE(SUBSTR(COALESCE(pay.threedigitzip, '000'), 1, 3))						    AS ptnt_zip3_cd,
    --------------------------------------------------------------------------------------------------
    --- lab_test_smpl_collctn_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(lts.specimencollectiondateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(lts.specimencollectiondateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(lts.specimencollectiondateid, '%Y%m%d') AS DATE)
    END                                                                                   AS lab_test_smpl_collctn_dt,
    --------------------------------------------------------------------------------------------------
    --- lab_result_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(lts.resultdatetimeid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(lts.resultdatetimeid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(lts.resultdatetimeid, '%Y%m%d') AS DATE)
    END                                                                                   AS lab_result_dt,
    --------------------------------------------------------------------------------------------------
    --- lab_test_nm
    --------------------------------------------------------------------------------------------------
    dlts.testdescription                                                                   AS lab_test_nm,
    CLEAN_UP_NUMERIC_CODE(dlts.loinccode)                                                  AS lab_test_loinc_cd,
    lts.resultvalue                                                                        AS lab_result_msrmt,
    drum.resultuom                                                                         AS lab_result_uom,
    --------------------------------------------------------------------------------------------------
    --- lab_result_abnorm_flg
    --------------------------------------------------------------------------------------------------
    CASE
      WHEN UPPER(dtan.abnormalitycode) IN ('H', 'HH', 'LL', 'L', '>', 'A', '<', 'AA' ) THEN 'Y'
      WHEN UPPER(dtan.abnormalitycode) IN ('Normal')                                   THEN 'N'
      ELSE NULL
    END                                                                                     AS lab_result_abnorm_flg,
    lts.refrange                                                                           AS lab_result_ref_rng_txt,
    dlrs.statusdescription                                                                 AS lab_test_stat_cd,
	CASE
	    WHEN dlrs.statusdescription IS NOT NULL THEN 'RESULT_STATUS'
	ELSE NULL
	END			    																		AS lab_test_stat_cd_qual,
    --------------------------------------------------------------------------------------------------
    --- data_captr_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(lts.reporteddateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(lts.reporteddateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(lts.reporteddateid, '%Y%m%d') AS DATE)
    END                                                                                   AS data_captr_dt,
	'fact_lab_test'																		  AS prmy_src_tbl_nm,
	'156'																			      AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(lts.specimencollectiondateid, '%Y%m%d') AS DATE)  < '{AVAILABLE_START_DATE}'
          OR CAST(EXTRACT_DATE(lts.specimencollectiondateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}'                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(lts.specimencollectiondateid, 1, 4), '-',
	                SUBSTR(lts.specimencollectiondateid, 5, 2)
                )
    END                                                                         AS part_mth

FROM factlabtest lts
LEFT OUTER JOIN matching_payload pay            ON lts.residentid          = pay.personid              AND COALESCE(lts.residentid, '0')             <> '0'
LEFT OUTER JOIN dimlabreportstatus dlrs         ON lts.labreportstatusid   = dlrs.labreportstatusid    AND COALESCE(lts.labreportstatusid, '0')    <> '0'
LEFT OUTER JOIN dimlabtest dlts                 ON lts.labtestid           = dlts.labtestid            AND COALESCE(lts.labtestid, '0')             <> '0'
LEFT OUTER JOIN dimlabtestcondition dtcd        ON lts.labtestconditionid  = dtcd.labtestconditionid   AND COALESCE(lts.labtestconditionid, '0')   <> '0'
LEFT OUTER JOIN dimlabtestabnormality dtan      ON lts.labtestabnormalityid= dtan.labtestabnormalityid AND COALESCE(lts.labtestabnormalityid, '0') <> '0'
LEFT OUTER JOIN dimlabresultuom drum            ON lts.labresultuomid      = drum.labresultuomid       AND COALESCE(lts.labresultuomid, '0')       <> '0'
WHERE TRIM(lower(COALESCE(lts.specimencollectiondateid, 'empty'))) <> 'specimencollectiondateid'