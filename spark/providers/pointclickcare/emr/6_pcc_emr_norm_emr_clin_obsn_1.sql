SELECT 
    /* hv_clin_obsn_id */
    CASE 
        WHEN COALESCE(alg.organization_id, alg.fact_allergy_id) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(alg.organization_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(alg.fact_allergy_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(alg.input_file_name, '/')[SIZE(SPLIT(alg.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	dorg.organization_code                                                                  AS vdr_org_id,
	alg.fact_allergy_id                                                                     AS vdr_clin_obsn_id,
	/* vdr_clin_obsn_id_qual */
	CASE
	    WHEN alg.fact_allergy_id IS NOT NULL
	        THEN 'FACT_ALLERGY_ID'
        ELSE NULL
	END                                                                                     AS vdr_clin_obsn_id_qual,
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
	        CAST(EXTRACT_DATE(alg.onset_date_id, '%Y%m%d') AS DATE),
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
	CAST(NULL AS DATE)                                                                      AS clin_obsn_dt,
	/* clin_obsn_prov_npi */
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
	END                                                                                     AS clin_obsn_prov_npi,
	/* clin_obsn_prov_qual */
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
	END                                                                                     AS clin_obsn_prov_qual,
	/* clin_obsn_prov_alt_speclty_id */
    ARRAY
        (
            NULL,
            dstf_pcp.profession_type
        )[prov_explode.idx]                                                                 AS clin_obsn_prov_alt_speclty_id,
	/* clin_obsn_prov_alt_speclty_id_qual */
	CASE
	    WHEN ARRAY
                (
                    NULL,
                    dstf_pcp.profession_type
                )[prov_explode.idx] IS NOT NULL 
	        THEN 'PROFESSION_TYPE'
	    ELSE NULL
	END			    																		AS clin_obsn_prov_alt_speclty_id_qual,
	/* clin_obsn_prov_frst_nm */
    ARRAY
        (
            NULL,
            dstf_pcp.person_name
        )[prov_explode.idx]                                                                 AS clin_obsn_prov_frst_nm,
    dfcl.facility_name                                                                      AS clin_obsn_prov_fclty_nm,
    dfcl.address1                                                                           AS clin_obsn_prov_addr_1_txt,
    dfcl.address2                                                                           AS clin_obsn_prov_addr_2_txt,
    VALIDATE_STATE_CODE(dfcl.prov_state)                                                    AS clin_obsn_prov_state_cd,
    dfcl.postal_zip_code                                                                    AS clin_obsn_prov_zip_cd,
	/* clin_obsn_onset_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(alg.onset_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS clin_obsn_onset_dt,
	/* clin_obsn_typ_cd */
	CASE
	    WHEN COALESCE
	            (
	                dala.allergy_severity,
                    dala.allergy_type,
                    dalc.allergy_category
	            ) IS NULL
            THEN NULL
        ELSE CONCAT
                (
                    CASE
                        WHEN COALESCE
                                (
                                    dala.allergy_severity,
                                    dala.allergy_type
                                ) IS NOT NULL
                         AND dalc.allergy_category IS NOT NULL
                            THEN TRIM(CONCAT
                                        (
                                            COALESCE(dala.allergy_severity, ''), ' ',
                                            COALESCE(dala.allergy_type, ''), ' - ',
                                            COALESCE(dalc.allergy_category, '')
                                        ))
                        WHEN COALESCE
                                (
                                    dala.allergy_severity,
                                    dala.allergy_type
                                ) IS NOT NULL
                            THEN TRIM(CONCAT
                                        (
                                            COALESCE(dala.allergy_severity, ''), ' ',
                                            COALESCE(dala.allergy_type, ''), ' - '
                                        ))
                        ELSE dalc.allergy_category
                    END
                )
    END                                                                                     AS clin_obsn_typ_cd,
    dalg.allergen                                                                           AS clin_obsn_alt_cd,
    /* clin_obsn_alt_cd_qual */
    CASE
        WHEN dalg.allergen IS NOT NULL
            THEN 'ALLERGEN'
        ELSE NULL
    END                                                                                     AS clin_obsn_alt_cd_qual,
    /* clin_obsn_msrmt */
    CASE
        WHEN COALESCE
                (
                    dalr.allergy_reaction,
                    dalr.allergy_subreaction
                ) IS NOT NULL
            THEN SUBSTR(CONCAT
                        (
                            CASE 
                                WHEN dalr.allergy_reaction IS NOT NULL
                                    THEN ' - '
                            END,
                            COALESCE(dalr.allergy_reaction, ''),
                            CASE 
                                WHEN dalr.allergy_subreaction IS NOT NULL
                                    THEN ' - '
                            END,
                            COALESCE(dalr.allergy_subreaction, '')
                        ), 4)
    END                                                                                     AS clin_obsn_msrmt,
    CAST(NULL AS STRING)                                                                    AS clin_obsn_uom,
	'fact_allergy'																		    AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(alg.onset_date_id, '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(alg.onset_date_id, 1, 4), '-',
	                SUBSTR(alg.onset_date_id, 5, 2)
                )
	END																					    AS part_mth
 FROM alg
 LEFT OUTER JOIN dfcl
              ON alg.facility_id = dfcl.facility_id
             AND COALESCE(alg.facility_id, '0') <> '0'
 LEFT OUTER JOIN dclt
              ON alg.client_id = dclt.client_id
             AND COALESCE(alg.client_id, '0') <> '0'
 LEFT OUTER JOIN matching_payload pay
              ON dclt.resident_id = pay.personid
             AND COALESCE(dclt.resident_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pcp
              ON dclt.primary_physician_id = dstf_pcp.staff_id
             AND COALESCE(dclt.primary_physician_id, '0') <> '0'
 LEFT OUTER JOIN dorg
              ON alg.organization_id = dorg.organization_id
             AND COALESCE(alg.organization_id, '0') <> '0'
 LEFT OUTER JOIN dalg
              ON alg.allergen_id = dalg.allergen_id
             AND COALESCE(alg.allergen_id, '0') <> '0'
 LEFT OUTER JOIN dala
              ON alg.allergy_attribute_id = dala.allergy_attribute_id
             AND COALESCE(alg.allergy_attribute_id, '0') <> '0'
 LEFT OUTER JOIN dalc
              ON alg.allergy_category_id = dalc.allergy_category_id
             AND COALESCE(alg.allergy_category_id, '0') <> '0'
 LEFT OUTER JOIN dalr
              ON alg.allergy_reaction_id = dalr.allergy_reaction_id
             AND COALESCE(alg.allergy_reaction_id, '0') <> '0'
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
  AND TRIM(UPPER(COALESCE(alg.client_id, ''))) <> 'CLIENT_ID'
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
