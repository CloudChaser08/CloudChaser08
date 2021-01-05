SELECT
    --------------------------------------------------------------------------------------------------
    ---  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(cpf.organizationid, cpf.factcareprofileid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(cpf.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(cpf.factcareprofileid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(cpf.input_file_name, '/')[SIZE(SPLIT(cpf.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_clin_obsn_id and vdr_clin_obsn_id_qual
    --------------------------------------------------------------------------------------------------
	cpf.factcareprofileid                                                                AS vdr_clin_obsn_id,
 	CASE
 	    WHEN cpf.factcareprofileid IS NOT NULL THEN 'FACT_CARE_PROFILE_ID'
    ELSE NULL
 	END                                                                                  AS vdr_clin_obsn_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
 	CASE
 	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))       THEN pay.hvid
 	    WHEN 0 <> LENGTH(TRIM(COALESCE(cpf.residentid, ''))) THEN CONCAT('156_', cpf.residentid)
 	ELSE NULL
 	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
 	CAST(
 	    CAP_YEAR_OF_BIRTH
 	    (
 	        pay.age,
 	        NULL,
 	        pay.yearofbirth
 	    )
 	  AS INT) AS ptnt_birth_yr,
    --------------------------------------------------------------------------------------------------
    --- ptnt_gender_cd
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				    	 AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(pay.state))													 AS ptnt_state_cd,
    MASK_ZIP_CODE(SUBSTR(COALESCE(pay.threedigitzip, '000'), 1, 3))						     AS ptnt_zip3_cd,
    CAST(NULL AS DATE)                                                                       AS clin_obsn_dt,
    CAST(NULL AS DATE)                                                                       AS clin_obsn_onset_dt,
    dcpf.category                                                                            AS clin_obsn_typ_cd,
    CAST(NULL AS STRING)                                                                     AS clin_obsn_alt_cd,
    CAST(NULL AS STRING)                                                                     AS clin_obsn_alt_cd_qual,
    dcpf.response                                                                            AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                     AS clin_obsn_uom,
    'fact_care_profile'																		 AS prmy_src_tbl_nm,
    '156'																			         AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- hard coded because there is no dates in this fact table
    --------------------------------------------------------------------------------------------------
    '0_PREDATES_HVM_HISTORY'                                                                 AS part_mth
FROM factcareprofile cpf
LEFT OUTER JOIN matching_payload pay            ON cpf.residentid            = pay.personid               AND COALESCE(cpf.residentid, '0')            <> '0'
LEFT OUTER JOIN dimorganization dorg            ON cpf.organizationid        = dorg.organizationid        AND COALESCE(cpf.organizationid, '0')        <> '0'
LEFT OUTER JOIN dimcareprofilequestion dcpf     ON cpf.careprofilequestionid = dcpf.careprofilequestionid AND COALESCE(cpf.careprofilequestionid, '0') <> '0'
WHERE TRIM(lower(COALESCE(cpf.careprofilequestionid, 'empty'))) <> 'careprofilequestionid'
