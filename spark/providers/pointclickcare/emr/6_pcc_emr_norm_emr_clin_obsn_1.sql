SELECT
    --------------------------------------------------------------------------------------------------
    ---  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(alg.organizationid, alg.factallergyid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(alg.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(alg.factallergyid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_clin_obsn_id,

    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(alg.input_file_name, '/')[SIZE(SPLIT(alg.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_clin_obsn_id and vdr_clin_obsn_id_qual
    --------------------------------------------------------------------------------------------------
	alg.factallergyid                                                                      AS vdr_clin_obsn_id,
	CASE
	    WHEN alg.factallergyid IS NOT NULL THEN 'FACT_ALLERGY_ID'
    ELSE NULL
	END                                                                                     AS vdr_clin_obsn_id_qual,
    ---------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))         THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(alg.residentid, ''))) THEN CONCAT('156_', alg.residentid)
	    ELSE NULL
	END																				        AS hvid,
    ---------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
    CAST(
        CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE),
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
	CAST(NULL AS DATE)                                                                      AS clin_obsn_dt,
    --------------------------------------------------------------------------------------------------
    --- clin_obsn_onset_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE)
    END                                                                                   AS clin_obsn_onset_dt,
    --------------------------------------------------------------------------------------------------
    --- clin_obsn_typ_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN COALESCE(dala.allergyseverity, dala.allergytype, dalc.allergycategory) IS NULL THEN NULL
        ELSE CONCAT
                (
                    CASE
                        WHEN COALESCE(dala.allergyseverity, dala.allergytype) IS NOT NULL AND dalc.allergycategory IS NOT NULL
                            THEN TRIM(CONCAT
                                        (
                                            COALESCE(dala.allergyseverity, ''), ' ',
                                            COALESCE(dala.allergytype, ''), ' - ',
                                            COALESCE(dalc.allergycategory, '')
                                        ))
                        WHEN COALESCE( dala.allergyseverity,dala.allergytype) IS NOT NULL
                            THEN TRIM(CONCAT
                                        (
                                            COALESCE(dala.allergyseverity, ''), ' ',
                                            COALESCE(dala.allergytype, ''), ' - '
                                        ))
                    ELSE dalc.allergycategory
                    END
                )
    END                                                                                     AS clin_obsn_typ_cd,
    dalg.allergen                                                                           AS clin_obsn_alt_cd,
    --------------------------------------------------------------------------------------------------
    --- clin_obsn_alt_cd_qual
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN dalg.allergen IS NOT NULL THEN 'ALLERGEN'
    ELSE NULL
    END                                                                                     AS clin_obsn_alt_cd_qual,
    --------------------------------------------------------------------------------------------------
    --- clin_obsn_msrmt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(dalr.allergyreaction, dalr.allergysubreaction) IS NOT NULL
            THEN SUBSTR(CONCAT
                        (
                            CASE
                                WHEN dalr.allergyreaction IS NOT NULL    THEN ' - '
                            END,
                            COALESCE(dalr.allergyreaction, ''),
                            CASE
                                WHEN dalr.allergysubreaction IS NOT NULL THEN ' - '
                            END,
                            COALESCE(dalr.allergysubreaction, '')
                        ), 4)
    END                                                                                     AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
	'fact_allergy'																		    AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE)  < '{AVAILABLE_START_DATE}'
          OR CAST(EXTRACT_DATE(alg.onsetdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}'                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(alg.onsetdateid, 1, 4), '-',
	                SUBSTR(alg.onsetdateid, 5, 2)
                )
    END                                                                         AS part_mth
FROM factallergy alg
LEFT OUTER JOIN dimorganization dorg            ON alg.organizationid      = dorg.organizationid     AND COALESCE(alg.organizationid, '0') <> '0'
LEFT OUTER JOIN matching_payload pay            ON alg.residentid          = pay.personid            AND COALESCE(alg.residentid, '0') <> '0'
LEFT OUTER JOIN dimallergen dalg                ON alg.allergenid          = dalg.allergenid         AND COALESCE(alg.allergenid, '0') <> '0'
LEFT OUTER JOIN dimallergyattribute dala              ON alg.allergyattributeid  = dala.allergyattributeid AND COALESCE(alg.allergyattributeid, '0') <> '0'
LEFT OUTER JOIN dimallergycategory dalc         ON alg.allergycategoryid   = dalc.allergycategoryid  AND COALESCE(alg.allergycategoryid, '0') <> '0'
LEFT OUTER JOIN dimallergyreaction dalr         ON alg.allergyreactionid   = dalr.allergyreactionid  AND COALESCE(alg.allergyreactionid, '0') <> '0'
