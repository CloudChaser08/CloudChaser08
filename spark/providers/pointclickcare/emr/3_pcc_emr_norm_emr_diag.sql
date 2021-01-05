SELECT
DISTINCT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_diag_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(dgn.organizationid, dgn.factdiagnosisid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(dgn.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(dgn.factdiagnosisid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_diag_id,

    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(dgn.input_file_name, '/')[SIZE(SPLIT(dgn.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_diag_id and vdr_diag_id_qual
    --------------------------------------------------------------------------------------------------
	dgn.factdiagnosisid                                                                   AS vdr_diag_id,
	CASE
	    WHEN dgn.factdiagnosisid IS NOT NULL THEN 'FACT_DIAGNOSIS_ID'
    ELSE NULL
	END                                                                                     AS vdr_diag_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))        THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(dgn.residentid, '')))  THEN CONCAT('156_', dgn.residentid)
	ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
	CAST(
	    CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE),
	        pay.yearofbirth
	   )
	 AS INT)                                                                                AS ptnt_birth_yr,
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
    --- diag_onset_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)
    END                                                                                   AS diag_onset_dt,
    --------------------------------------------------------------------------------------------------
    --- diag_resltn_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(dgn.resolveddateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(dgn.resolveddateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(dgn.resolveddateid, '%Y%m%d') AS DATE)
    END                                                                                   AS diag_resltn_dt,
    --------------------------------------------------------------------------------------------------
    --- diag_cd
    --------------------------------------------------------------------------------------------------
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        ddgn.icdcode,
	        '02',
	        CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)
	    )                                                                                   AS diag_cd,
    --------------------------------------------------------------------------------------------------
    --- diag_cd_qual
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN ddgn.icdcode IS NOT NULL THEN '02'
    ELSE NULL
	END                                                                                     AS diag_cd_qual,
    --------------------------------------------------------------------------------------------------
    --- prmy_diag_flg
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(dgn.isprincipaldiagnosisind, 'X') = '0' THEN 'N'
        WHEN COALESCE(dgn.isprincipaldiagnosisind, 'X') = '1' THEN 'Y'
        ELSE NULL
    END                                                                                     AS prmy_diag_flg,
    --------------------------------------------------------------------------------------------------
    --- admt_diag_flg
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(dgn.isadmissiondiagnosisind, 'X') = '0' THEN 'N'
        WHEN COALESCE(dgn.isadmissiondiagnosisind, 'X') = '1' THEN 'Y'
        ELSE NULL
    END                                                                                     AS admt_diag_flg,
    CASE
        WHEN ddgr.diagnosisrank IS NOT NULL
         AND ddgc.diagnosisclassification IS NOT NULL  THEN CONCAT('DIAGNOSIS_RANK: ', ddgr.diagnosisrank, ' | ', 'DIAGNOSIS_CLASSIFICATION: ', ddgc.diagnosisclassification)
        WHEN ddgr.diagnosisrank IS NOT NULL            THEN CONCAT('DIAGNOSIS_RANK: ', ddgr.diagnosisrank)
        WHEN ddgc.diagnosisclassification IS NOT NULL THEN CONCAT('DIAGNOSIS_CLASSIFICATION: ', ddgc.diagnosisclassification)
    ELSE NULL
    END                                                                                     AS diag_grp_txt,
	'fact_diagnosis'																		AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
    CASE
        WHEN CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)  < '{AVAILABLE_START_DATE}'
          OR CAST(EXTRACT_DATE(dgn.onsetdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}'                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(dgn.onsetdateid, 1, 4), '-',
	                SUBSTR(dgn.onsetdateid, 5, 2)
                )
    END                                                                         AS part_mth

FROM factdiagnosis dgn
LEFT OUTER JOIN matching_payload pay                ON dgn.residentid                   = pay.personid         AND COALESCE(dgn.residentid, '0') <> '0'
LEFT OUTER JOIN dimorganization dorg                ON dgn.organizationid               = dorg.organizationid AND COALESCE(dgn.organizationid, '0') <> '0'
LEFT OUTER JOIN dimdiagnosis ddgn                   ON dgn.diagnosisid                  = ddgn.diagnosisid    AND COALESCE(dgn.diagnosisid, '0') <> '0'
LEFT OUTER JOIN dimdiagnosisrank ddgr               ON dgn.diagnosisrankid              = ddgr.diagnosisrankid  AND COALESCE(dgn.diagnosisrankid, '0') <> '0'
LEFT OUTER JOIN dimdiagnosisclassification ddgc     ON dgn.diagnosisclassificationid    = ddgc.diagnosisclassificationid  AND COALESCE(dgn.diagnosisclassificationid, '0') <> '0'
WHERE TRIM(lower(COALESCE(dgn.onsetdateid, 'empty'))) <> 'onsetdateid'
