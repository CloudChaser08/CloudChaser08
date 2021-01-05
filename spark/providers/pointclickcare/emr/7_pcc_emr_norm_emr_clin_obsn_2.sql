SELECT
    --------------------------------------------------------------------------------------------------
    ---  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(obs.organizationid, obs.factobservationid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(obs.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(obs.factobservationid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(obs.input_file_name, '/')[SIZE(SPLIT(obs.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_clin_obsn_id and vdr_clin_obsn_id_qual
    --------------------------------------------------------------------------------------------------
	obs.factobservationid                                                                 AS vdr_clin_obsn_id,
    CASE
        WHEN obs.factobservationid IS NOT NULL THEN 'FACT_OBSERVATION_ID'
        ELSE NULL
    END                                                                                     AS vdr_clin_obsn_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))          THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(obs.residentid, '')))  THEN CONCAT('156_', obs.residentid)
    ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
    CAST(
        CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE),
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
    --- clin_obsn_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE)
    END                                                                                     AS clin_obsn_dt,
    CAST(NULL AS DATE)                                                                      AS clin_obsn_onset_dt,
    ref.gen_ref_1_txt                                                                       AS clin_obsn_typ_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_alt_cd_qual,
    obs.observationvalueimperial                                                            AS clin_obsn_msrmt,
    ref.gen_ref_2_txt                                                                       AS clin_obsn_uom,
	'fact_observation'																		AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE)  < '{AVAILABLE_START_DATE}'
          OR CAST(EXTRACT_DATE(obs.observationdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}'                   THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(obs.observationdateid, 1, 4), '-',
	                SUBSTR(obs.observationdateid, 5, 2)
                )
    END                                                                         AS part_mth
FROM factobservation obs
LEFT OUTER JOIN matching_payload pay        ON obs.residentid           = pay.personid AND COALESCE(obs.residentid, '0') <> '0'
LEFT OUTER JOIN dimorganization dorg        ON obs.organizationid       = dorg.organizationid AND COALESCE(obs.organizationid, '0') <> '0'
LEFT OUTER JOIN dimobservation dobs         ON obs.observationid        = dobs.observationid  AND COALESCE(obs.observationid, '0') <> '0'
/* Only retrieve valid observation translations. */
INNER JOIN ref_gen_ref ref                  ON ref.gen_ref_domn_nm      = 'pointclickcare_emr.observation'
                                                    AND UPPER(COALESCE(dobs.observation, '')) = UPPER(ref.gen_ref_cd)
WHERE
    --------------------------------------------------------------------------------------------------
    --- Select only non-blood-pressure rows.
    --------------------------------------------------------------------------------------------------
      COALESCE(obs.observationid, '0') NOT IN ('3', '4')
    --------------------------------------------------------------------------------------------------
    --- Select only where there's a measurement.
    --------------------------------------------------------------------------------------------------
  AND obs.observationvalueimperial IS NOT NULL
  AND TRIM(lower(COALESCE(obs.observationdateid, 'empty'))) <> 'observationdateid'
