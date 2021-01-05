SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(imm.organizationid, imm.factimmunizationid) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(imm.organizationid, 'UNAVAILABLE'),
                        '_',
                        COALESCE(imm.factimmunizationid, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(imm.input_file_name, '/')[SIZE(SPLIT(imm.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	UPPER(dorg.OrganizationCode)                                                            AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_proc_id and vdr_proc_id_qual
    --------------------------------------------------------------------------------------------------
	imm.factimmunizationid                                                                  AS vdr_proc_id,
	CASE
	    WHEN imm.factimmunizationid IS NOT NULL THEN 'FACT_IMMUNIZATION_ID'
    ELSE NULL
	END                                                                                     AS vdr_proc_id_qual,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))         THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(imm.residentid, ''))) THEN CONCAT('156_', imm.residentid)
	    ELSE NULL
	END																				        AS hvid,
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------
	CAST(
	    CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE),
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
    --- proc_dt
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE)  < '{EARLIEST_SERVICE_DATE}'
          OR CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE)
    END                                                                                     AS proc_dt,
    --------------------------------------------------------------------------------------------------
    --- proc_cd ans qualifier
    --------------------------------------------------------------------------------------------------
	CLEAN_UP_PROCEDURE_CODE(dvcx.vaccinecode)                                               AS proc_cd,
	CASE
	    WHEN dvcx.vaccinecode IS NOT NULL THEN 'CVX'
    ELSE NULL
	END                                                                                     AS proc_cd_qual,
    --------------------------------------------------------------------------------------------------
    --- proc_typ_cd and qualifier
    --------------------------------------------------------------------------------------------------
	dimm.immunization                                                                       AS proc_typ_cd,
    CASE
        WHEN dimm.immunization IS NULL THEN NULL
    ELSE 'IMMUNIZATION_TYPE'
    END                                                                                     AS proc_typ_cd_qual,
	'fact_immunization'																		AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
    CASE
        WHEN CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE)  < '{AVAILABLE_START_DATE}'
          OR CAST(EXTRACT_DATE(imm.immunizationdateid, '%Y%m%d') AS DATE)  > '{VDR_FILE_DT}'                    THEN '0_PREDATES_HVM_HISTORY'
    ELSE  CONCAT
	            (
	                SUBSTR(imm.immunizationdateid, 1, 4), '-',
	                SUBSTR(imm.immunizationdateid, 5, 2)
                )
    END                                                                         AS part_mth

FROM factimmunization imm
LEFT OUTER JOIN matching_payload pay        ON imm.residentid       = pay.personid        AND COALESCE(imm.residentid, '0') <> '0'
LEFT OUTER JOIN dimorganization dorg        ON imm.organizationid   = dorg.organizationid AND COALESCE(imm.organizationid, '0') <> '0'
LEFT OUTER JOIN dimimmunization dimm        ON imm.immunizationid   = dimm.immunizationid AND COALESCE(imm.immunizationid, '0') <> '0'
LEFT OUTER JOIN dimvaccine dvcx             ON imm.vaccineid        = dvcx.vaccineid      AND COALESCE(imm.vaccineid, '0') <> '0'
WHERE TRIM(lower(COALESCE(imm.immunizationdateid, 'empty'))) <> 'immunizationdateid'
