SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_medctn_id */
    CASE 
        WHEN COALESCE(med.organization_id, med.fact_medication_order_id) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(med.organization_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(med.fact_medication_order_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(med.input_file_name, '/')[SIZE(SPLIT(med.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	dorg.organization_code                                                                  AS vdr_org_id,
	med.fact_medication_order_id                                                            AS vdr_medctn_ord_id,
	/* vdr_medctn_id_qual */
	CASE
	    WHEN med.fact_medication_order_id IS NOT NULL
	        THEN 'FACT_MEDICATION_ORDER_ID'
        ELSE NULL
	END                                                                                     AS vdr_medctn_ord_id_qual,
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
	        CAST(EXTRACT_DATE(med.medication_order_date_id, '%Y%m%d') AS DATE),
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
	/* medctn_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(med.medication_order_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_ord_dt,
	/* medctn_prov_npi */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_pre.npi,
                    dfcl_phr.facility_npi
                )[prov_explode.idx] IS NOT NULL
            THEN CLEAN_UP_NPI_CODE
                    (
                        ARRAY
                        (
                            dfcl.facility_npi,
                            dstf_pcp.npi,
                            dstf_pre.npi,
                            dfcl_phr.facility_npi
                        )[prov_explode.idx]
                    )
	    ELSE NULL
	END                                                                                     AS medctn_prov_npi,
	/* medctn_prov_qual */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_pre.npi,
                    dfcl_phr.facility_npi
                )[prov_explode.idx] IS NOT NULL
            THEN ARRAY
                    (
                        'RENDERING_FACILITY',
                        'PRIMARY_CARE_PHYSICIAN',
                        'PRESCRIBING_PROVIDER',
                        'PHARMACY_FACILITY'
                    )[prov_explode.idx]
	    ELSE NULL
	END                                                                                     AS medctn_prov_qual,
	/* medctn_prov_alt_speclty_id */
    ARRAY
        (
            NULL,
            dstf_pcp.profession_type,
            dstf_pre.profession_type,
            NULL
        )[prov_explode.idx]                                                                 AS medctn_prov_alt_speclty_id,
	/* medctn_prov_alt_speclty_id_qual */
	CASE
	    WHEN ARRAY
                (
                    NULL,
                    dstf_pcp.profession_type,
                    dstf_pre.profession_type,
                    NULL
                )[prov_explode.idx] IS NOT NULL 
	        THEN 'PROFESSION_TYPE'
	    ELSE NULL
	END			    																		AS medctn_prov_alt_speclty_id_qual,
	/* medctn_prov_frst_nm */
    ARRAY
        (
            NULL,
            dstf_pcp.person_name,
            dstf_pre.person_name,
            NULL
        )[prov_explode.idx]                                                                 AS medctn_prov_frst_nm,
    /* medctn_prov_fclty_nm */
    ARRAY
        (
            dfcl.facility_name,
            dfcl.facility_name,
            dfcl.facility_name,
            dfcl_phr.facility_name
        )[prov_explode.idx]                                                                 AS medctn_prov_fclty_nm,
    /* medctn_prov_addr_1_txt */
    ARRAY
        (
            dfcl.address1,
            dfcl.address1,
            dfcl.address1,
            dfcl_phr.address1
        )[prov_explode.idx]                                                                 AS medctn_prov_addr_1_txt,
    /* medctn_prov_addr_2_txt */
    ARRAY
        (
            dfcl.address2,
            dfcl.address2,
            dfcl.address2,
            dfcl_phr.address2
        )[prov_explode.idx]                                                                 AS medctn_prov_addr_2_txt,
    /* medctn_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            ARRAY
                (
                    dfcl.prov_state,
                    dfcl.prov_state,
                    dfcl.prov_state,
                    dfcl_phr.prov_state
                )[prov_explode.idx]
        )                                                                                   AS medctn_prov_state_cd,
    /* medctn_prov_zip_cd */
    ARRAY
        (
            dfcl.postal_zip_code,
            dfcl.postal_zip_code,
            dfcl.postal_zip_code,
            dfcl_phr.postal_zip_code
        )[prov_explode.idx]                                                                 AS medctn_prov_zip_cd,
	/* medctn_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(med.start_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_start_dt,
	/* medctn_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(med.end_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_end_dt,
	ddru.gpi                                                                                AS medctn_alt_cd,
	/* medctn_alt_cd_qual */
	CASE
	    WHEN ddru.gpi IS NOT NULL
	        THEN 'GPI'
        ELSE NULL
	END                                                                                     AS medctn_alt_cd_qual,
	ddru.drug_name                                                                          AS medctn_brd_nm,
	ddru.lvl_4_drug_base_name                                                               AS medctn_genc_nm,
	COALESCE(ddru.form, ddru.lvl_6_drug_form)                                               AS medctn_admin_form_nm,
	/*  */
	CASE
	    WHEN COALESCE(ddru.strength, ddru.strength_unit) IS NOT NULL
	        THEN TRIM(CONCAT
    	                (
    	                    COALESCE(ddru.strength, ''), ' ',
    	                    COALESCE(ddru.strength_unit, '')
    	                ))
	    ELSE ddru.lvl_7_drug_strength
	END                                                                                     AS medctn_strth_txt,
	'fact_medication_order'																	AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(med.medication_order_date_id, '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(med.medication_order_date_id, 1, 4), '-',
	                SUBSTR(med.medication_order_date_id, 5, 2)
                )
	END																					    AS part_mth
 FROM med
 LEFT OUTER JOIN dfcl
              ON med.facility_id = dfcl.facility_id
             AND COALESCE(med.facility_id, '0') <> '0'
 LEFT OUTER JOIN dclt
              ON med.client_id = dclt.client_id
             AND COALESCE(med.client_id, '0') <> '0'
 LEFT OUTER JOIN matching_payload pay
              ON dclt.resident_id = pay.personid
             AND COALESCE(dclt.resident_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pcp
              ON dclt.primary_physician_id = dstf_pcp.staff_id
             AND COALESCE(dclt.primary_physician_id, '0') <> '0'
 LEFT OUTER JOIN dorg
              ON med.organization_id = dorg.organization_id
             AND COALESCE(med.organization_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pre
              ON med.physician_staff_id = dstf_pre.staff_id
             AND COALESCE(med.physician_staff_id, '0') <> '0'
 LEFT OUTER JOIN dfcl dfcl_phr
              ON med.pharmacy_facility_id = dfcl_phr.facility_id
             AND COALESCE(med.pharmacy_facility_id, '0') <> '0'
 LEFT OUTER JOIN ddru
              ON med.ddid = ddru.ddid
             AND COALESCE(med.ddid, '0') <> '0'
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS idx) AS prov_explode
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
  AND TRIM(UPPER(COALESCE(med.client_id, ''))) <> 'CLIENT_ID'
  /* Provider selection. */
  /* Retrieve where the source NPI is populated, */
  /* or all of the NPIs are empty and this is the first one. */
  AND 
    (
        ARRAY
            (
                dfcl.facility_npi,
                dstf_pcp.npi,
                dstf_pre.npi,
                dfcl_phr.facility_npi
            )[prov_explode.idx] IS NOT NULL
    OR
        (
            COALESCE
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_pre.npi,
                    dfcl_phr.facility_npi
                ) IS NULL
        AND idx = 0
        )
    )
