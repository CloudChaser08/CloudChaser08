SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_diag_id */
    CASE 
        WHEN COALESCE(dgn.organization_id, dgn.fact_diagnosis_id) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(dgn.organization_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(dgn.fact_diagnosis_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(dgn.input_file_name, '/')[SIZE(SPLIT(dgn.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	dorg.organization_code                                                                  AS vdr_org_id,
	dgn.fact_diagnosis_id                                                                   AS vdr_diag_id,
	/* vdr_diag_id_qual */
	CASE
	    WHEN dgn.fact_diagnosis_id IS NOT NULL
	        THEN 'FACT_DIAGNOSIS_ID'
        ELSE NULL
	END                                                                                     AS vdr_diag_id_qual,
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
	        CAST(EXTRACT_DATE(dgn.onset_date_id, '%Y%m%d') AS DATE),
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
	/* diag_prov_npi */
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
	END                                                                                     AS diag_prov_npi,
	/* diag_prov_qual */
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
	END                                                                                     AS diag_prov_qual,
	/* diag_prov_alt_speclty_id */
    ARRAY
        (
            NULL,
            dstf_pcp.profession_type
        )[prov_explode.idx]                                                                 AS diag_prov_alt_speclty_id,
	/* diag_prov_alt_speclty_id_qual */
	CASE
	    WHEN ARRAY
                (
                    NULL,
                    dstf_pcp.profession_type
                )[prov_explode.idx] IS NOT NULL 
	        THEN 'PROFESSION_TYPE'
	    ELSE NULL
	END			    																		AS diag_prov_alt_speclty_id_qual,
	/* diag_prov_frst_nm */
    ARRAY
        (
            NULL,
            dstf_pcp.person_name
        )[prov_explode.idx]                                                                 AS diag_prov_frst_nm,
    dfcl.facility_name                                                                      AS diag_prov_fclty_nm,
    dfcl.address1                                                                           AS diag_prov_addr_1_txt,
    dfcl.address2                                                                           AS diag_prov_addr_2_txt,
    VALIDATE_STATE_CODE(dfcl.prov_state)                                                    AS diag_prov_state_cd,
    dfcl.postal_zip_code                                                                    AS diag_prov_zip_cd,
	/* diag_onset_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(dgn.onset_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_onset_dt,
	/* diag_resltn_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(dgn.resolved_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS diag_resltn_dt,
	/* diag_cd */
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        ddgn.icd_code,
	        '02',
	        dgn.onset_date_id
	    )                                                                                   AS diag_cd,
	/* diag_cd_qual */
	CASE
	    WHEN ddgn.icd_code IS NOT NULL
	        THEN '02'
        ELSE NULL
	END                                                                                     AS diag_cd_qual,
    /* prmy_diag_flg */
    CASE
        WHEN COALESCE(dgn.is_principal_diagnosis_ind, 'X') = '0'
            THEN 'N'
        WHEN COALESCE(dgn.is_principal_diagnosis_ind, 'X') = '1'
            THEN 'Y'
        ELSE NULL
    END                                                                                     AS prmy_diag_flg,
    /* admt_diag_flg */
    CASE
        WHEN COALESCE(dgn.is_admission_diagnosis_ind, 'X') = '0'
            THEN 'N'
        WHEN COALESCE(dgn.is_admission_diagnosis_ind, 'X') = '1'
            THEN 'Y'
        ELSE NULL
    END                                                                                     AS admt_diag_flg,
	'fact_diagnosis'																		AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(dgn.onset_date_id, '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(dgn.onset_date_id, 1, 4), '-',
	                SUBSTR(dgn.onset_date_id, 5, 2)
                )
	END																					    AS part_mth
 FROM dgn
 LEFT OUTER JOIN dfcl
              ON dgn.facility_id = dfcl.facility_id
             AND COALESCE(dgn.facility_id, '0') <> '0'
 LEFT OUTER JOIN dclt
              ON dgn.client_id = dclt.client_id
             AND COALESCE(dgn.client_id, '0') <> '0'
 LEFT OUTER JOIN matching_payload pay
              ON dclt.resident_id = pay.personid
             AND COALESCE(dclt.resident_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pcp
              ON dclt.primary_physician_id = dstf_pcp.staff_id
             AND COALESCE(dclt.primary_physician_id, '0') <> '0'
 LEFT OUTER JOIN dorg
              ON dgn.organization_id = dorg.organization_id
             AND COALESCE(dgn.organization_id, '0') <> '0'
 LEFT OUTER JOIN ddgn
              ON dgn.diagnosis_id = ddgn.diagnosis_id
             AND COALESCE(dgn.diagnosis_id, '0') <> '0'
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
  AND TRIM(UPPER(COALESCE(dgn.client_id, ''))) <> 'CLIENT_ID'
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
