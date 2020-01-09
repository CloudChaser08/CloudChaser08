SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_proc_id */
    CASE 
        WHEN COALESCE(imm.organization_id, imm.fact_immunization_id) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(imm.organization_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(imm.fact_immunization_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(imm.input_file_name, '/')[SIZE(SPLIT(imm.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	dorg.organization_code                                                                  AS vdr_org_id,
	imm.fact_immunization_id                                                                AS vdr_proc_id,
	/* vdr_proc_id_qual */
	CASE
	    WHEN imm.fact_immunization_id IS NOT NULL
	        THEN 'FACT_IMMUNIZATION_ID'
        ELSE NULL
	END                                                                                     AS vdr_proc_id_qual,
	/* hvid */
	CASE 
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, ''))) 
	        THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(dclt.resident_id, ''))) 
	        THEN CONCAT('156_', COALESCE(dclt.resident_id, 'NONE')) 
	    ELSE NULL 
	END																				        AS hvid,
	/* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
	        CAST(EXTRACT_DATE(imm.immunization_date_id, '%Y%m%d') AS DATE),
	        pay.yearofbirth
	    )																					AS ptnt_birth_yr,
	/* ptnt_gender_cd */
	CASE 
	    WHEN SUBSTR(UPPER(COALESCE(pay.gender, 'U')), 1, 1) IN ('F', 'M') 
	        THEN SUBSTR(UPPER(COALESCE(pay.gender, 'U')), 1, 1) 
	    ELSE 'U' 
	END																				    	AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(pay.state))													AS ptnt_state_cd,
	MASK_ZIP_CODE(SUBSTR(COALESCE(pay.threedigitzip, '000'), 1, 3))						    AS ptnt_zip3_cd,
	/* proc_prov_npi */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi
                )[prov_explode.idx] IS NOT NULL
            THEN CLEAN_UP_NPI_CODE
                    (
                        ARRAY
                        (
                            dfcl.facility_npi,
                            dstf_pcp.npi
                        )[prov_explode.idx]
                    )
	    ELSE NULL
	END                                                                                     AS proc_prov_npi,
	/* proc_prov_qual */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi
                )[prov_explode.idx] IS NOT NULL
            THEN ARRAY
                    (
                        'RENDERING_FACILITY',
                        'PRIMARY_CARE_PHYSICIAN'
                    )[prov_explode.idx]
	    ELSE NULL
	END                                                                                     AS proc_prov_qual,
	/* proc_prov_alt_speclty_id */
    ARRAY
        (
            NULL,
            dstf_pcp.profession_type
        )[prov_explode.idx]                                                                 AS proc_prov_alt_speclty_id,
	/* proc_prov_alt_speclty_id_qual */
	CASE
	    WHEN ARRAY
                (
                    NULL,
                    dstf_pcp.profession_type
                )[prov_explode.idx] IS NOT NULL 
	        THEN 'PROFESSION_TYPE'
	    ELSE NULL
	END			    																		AS proc_prov_alt_speclty_id_qual,
	/* proc_prov_frst_nm */
    ARRAY
        (
            NULL,
            dstf_pcp.person_name
        )[prov_explode.idx]                                                                 AS proc_prov_frst_nm,
    dfcl.facility_name                                                                      AS proc_prov_fclty_nm,
    dfcl.address1                                                                           AS proc_prov_addr_1_txt,
    dfcl.address2                                                                           AS proc_prov_addr_2_txt,
    VALIDATE_STATE_CODE(dfcl.prov_state)                                                    AS proc_prov_state_cd,
    dfcl.postal_zip_code                                                                    AS proc_prov_zip_cd,
	/* proc_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(imm.immunization_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS proc_dt,
	CLEAN_UP_PROCEDURE_CODE(dvcx.vaccine_code)                                              AS proc_cd,
	/* proc_cd_qual */
	CASE
	    WHEN dvcx.vaccine_code IS NOT NULL
	        THEN 'CVX'
        ELSE NULL
	END                                                                                     AS proc_cd_qual,
	dimm.immunization                                                                       AS proc_typ_cd,
    /* proc_typ_cd_qual */
    CASE
        WHEN dimm.immunization IS NULL
            THEN NULL
        ELSE 'IMMUNIZATION_TYPE'
    END                                                                                     AS proc_typ_cd_qual,
	'fact_immunization'																		AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(imm.immunization_date_id, '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(imm.immunization_date_id, 1, 4), '-',
	                SUBSTR(imm.immunization_date_id, 5, 2)
                )
	END																					    AS part_mth
 FROM imm
 LEFT OUTER JOIN dfcl
              ON imm.facility_id = dfcl.facility_id
             AND COALESCE(imm.facility_id, '0') <> '0'
 LEFT OUTER JOIN dclt
              ON imm.client_id = dclt.client_id
             AND COALESCE(imm.client_id, '0') <> '0'
 LEFT OUTER JOIN matching_payload pay
              ON dclt.resident_id = pay.personid
             AND COALESCE(dclt.resident_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pcp
              ON dclt.primary_physician_id = dstf_pcp.staff_id
             AND COALESCE(dclt.primary_physician_id, '0') <> '0'
 LEFT OUTER JOIN dorg
              ON imm.organization_id = dorg.organization_id
             AND COALESCE(imm.organization_id, '0') <> '0'
 LEFT OUTER JOIN dimm
              ON imm.immunization_id = dimm.immunization_id
             AND COALESCE(imm.immunization_id, '0') <> '0'
 LEFT OUTER JOIN dvcx
              ON imm.vaccine_id = dvcx.vaccine_id
             AND COALESCE(imm.vaccine_id, '0') <> '0'
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS idx) AS prov_explode
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 156
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
              ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 156
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
              ON 1 = 1
WHERE EXISTS
/* Select only valid U.S. states and territories. */
    (
        SELECT 1
         FROM ref_geo_state sts
        WHERE UPPER(COALESCE(dfcl.prov_state, '')) = sts.geo_state_pstl_cd
    )
  /* Eliminate column header rows. */
  AND TRIM(UPPER(COALESCE(imm.client_id, ''))) <> 'CLIENT_ID'
  /* Provider selection. */
  /* Retrieve where the source NPI is populated, */
  /* or all of the NPIs are empty and this is the first one. */
  AND 
    (
        ARRAY
            (
                dfcl.facility_npi,
                dstf_pcp.npi
            )[prov_explode.idx] IS NOT NULL
    OR
        (
            COALESCE
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi
                ) IS NULL
        AND idx = 0
        )
    )
