SELECT 
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN COALESCE(cen.organizationid, cen.factcensusid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(cen.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(cen.factcensusid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(cen.input_file_name, '/')[SIZE(SPLIT(cen.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_enc_id and vdr_enc_id_qual
    --------------------------------------------------------------------------------------------------	
	cen.factcensusid                                                                        AS vdr_enc_id,
	CASE
	    WHEN cen.factcensusid IS NOT NULL THEN 'FACT_CENSUS_ID'
    ELSE NULL
	END                                                                                     AS vdr_enc_id_qual,
	CAST(NULL AS STRING)                                                                    AS vdr_alt_enc_id,
	CAST(NULL AS STRING)                                                                    AS vdr_alt_enc_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))          THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(cen.residentid, '')))  THEN CONCAT('156_', cen.residentid) 
	    ELSE NULL 
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------	
	CAST(
	    CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE),
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
        WHEN CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE)
    END                                                                                   AS enc_start_dt,
    --------------------------------------------------------------------------------------------------
    --- enc_end_dt
    --------------------------------------------------------------------------------------------------	    	    
	CAST(NULL AS DATE)                                                                    AS enc_end_dt,
    'CENSUS'                                                                              AS enc_typ_cd,
    CAST(NULL AS STRING)                                                                  AS enc_grp_txt,
	'fact_census'																		  AS prmy_src_tbl_nm,
	'156'																			      AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(cen.censuseffectivedateid, '%Y%m%d') AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(cen.censuseffectivedateid, 1, 4), '-',
	                SUBSTR(cen.censuseffectivedateid, 5, 2)
                )
    END                                                                         AS part_mth
 FROM factcensus cen
 LEFT OUTER JOIN matching_payload pay           ON cen.residentid             = pay.personid         AND COALESCE(cen.residentid, '0') <> '0'
 LEFT OUTER JOIN dimorganization dorg ON cen.organizationid         = dorg.organizationid  AND COALESCE(cen.organizationid, '0') <> '0'
WHERE TRIM(lower(COALESCE(cen.censuseffectivedateid, 'empty'))) <> 'censuseffectivedateid'
