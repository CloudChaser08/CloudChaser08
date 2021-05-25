SELECT
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(vis.organizationid, vis.factvisitid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(vis.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(vis.factvisitid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                  AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(vis.input_file_name, '/')[SIZE(SPLIT(vis.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_enc_id
    --------------------------------------------------------------------------------------------------
	vis.factvisitid                                                                         AS vdr_enc_id,
	CASE
	    WHEN vis.factvisitid IS NOT NULL THEN 'FACT_VISIT_ID'
        ELSE NULL
	END                                                                                     AS vdr_enc_id_qual,
    --------------------------------------------------------------------------------------------------
    --- vdr_alt_enc_id
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN vis.admitfactcensusid IS NOT NULL
	     AND vis.dischargefactcensusid IS NOT NULL THEN CONCAT(vis.admitfactcensusid, '-', vis.dischargefactcensusid)
	    WHEN vis.admitfactcensusid IS NOT NULL     THEN vis.admitfactcensusid
	    WHEN vis.dischargefactcensusid IS NOT NULL THEN vis.dischargefactcensusid
	    ELSE NULL
	END                                                                                     AS vdr_alt_enc_id,

	CASE
	    WHEN vis.admitfactcensusid IS NOT NULL
	     AND vis.dischargefactcensusid IS NOT NULL THEN 'ADMIT_FROM_CENSUS_ID-DISCHARGE_TO_CENSUS_ID'
	    WHEN vis.admitfactcensusid IS     NOT NULL THEN 'ADMIT_FROM_CENSUS_ID'
	    WHEN vis.dischargefactcensusid IS NOT NULL THEN 'DISCHARGE_TO_CENSUS_ID'
	    ELSE NULL
	END                                                                                     AS vdr_alt_enc_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))        THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(vis.residentid, ''))) THEN CONCAT('156_', vis.residentid)
	    ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
	CAST(
	CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE),
	        pay.yearofbirth
	    )
	    AS INT)                                                                             AS ptnt_birth_yr,
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
    --- enc_start_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE)
    END                                                                                   AS enc_start_dt,
    --------------------------------------------------------------------------------------------------
    --- enc_end_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(vis.dischargedateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(vis.dischargedateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(vis.dischargedateid, '%Y%m%d') AS DATE)
    END                                                                                   AS enc_end_dt,
    CAST(NULL AS STRING)                                                                  AS enc_typ_cd,
    --------------------------------------------------------------------------------------------------
    --- enc_grp_txt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN vis.lengthofstay IS NULL THEN NULL
    ELSE CONCAT('LENGTH_OF_STAY: ', vis.lengthofstay)
    END                                                                                     AS enc_grp_txt,
	'fact_visit'																		    AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(vis.admitdateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(vis.admitdateid, 1, 4), '-',
	                SUBSTR(vis.admitdateid, 5, 2)
                )
    END                                                                         AS part_mth
FROM factvisit vis
LEFT OUTER JOIN matching_payload pay			  ON vis.residentid     = pay.personid			AND COALESCE(vis.residentid, '0') <> '0'
LEFT OUTER JOIN dimorganization dorg ON vis.organizationid = dorg.organizationid	AND COALESCE(vis.organizationid, '0') <> '0'
WHERE TRIM(lower(COALESCE(vis.admitdateid, 'empty'))) <> 'admitdateid'
