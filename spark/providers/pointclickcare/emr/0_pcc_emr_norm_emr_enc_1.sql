SELECT 
    /* hv_enc_id */
    CASE 
        WHEN COALESCE(vis.organization_id, vis.fact_visit_id) IS NOT NULL
            THEN CONCAT
                    (
                        '156_',
                        COALESCE(vis.organization_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(vis.fact_visit_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(vis.input_file_name, '/')[SIZE(SPLIT(vis.input_file_name, '/')) - 1]              AS data_set_nm,
	511                                                                                     AS hvm_vdr_id,
	156                                                                                     AS hvm_vdr_feed_id,
	dorg.organization_code                                                                  AS vdr_org_id,
	vis.fact_visit_id                                                                       AS vdr_enc_id,
	/* vdr_enc_id_qual */
	CASE
	    WHEN vis.fact_visit_id IS NOT NULL
	        THEN 'FACT_VISIT_ID'
        ELSE NULL
	END                                                                                     AS vdr_enc_id_qual,
	/* vdr_alt_enc_id */
	CASE
	    WHEN vis.admit_fact_census_id IS NOT NULL
         AND vis.discharge_fact_census_id IS NOT NULL
            THEN CONCAT(vis.admit_fact_census_id, '-', vis.discharge_fact_census_id)
	    WHEN vis.admit_fact_census_id IS NOT NULL
	        THEN vis.admit_fact_census_id
	    WHEN vis.discharge_fact_census_id IS NOT NULL
	        THEN vis.discharge_fact_census_id
	    ELSE NULL
	END                                                                                     AS vdr_alt_enc_id,
	/* vdr_alt_enc_id_qual */
	CASE
	    WHEN vis.admit_fact_census_id IS NOT NULL
         AND vis.discharge_fact_census_id IS NOT NULL
            THEN 'ADMIT_FROM_CENSUS_ID-DISCHARGE_TO_CENSUS_ID'
	    WHEN vis.admit_fact_census_id IS NOT NULL
	        THEN 'ADMIT_FROM_CENSUS_ID'
	    WHEN vis.discharge_fact_census_id IS NOT NULL
	        THEN 'DISCHARGE_TO_CENSUS_ID'
	    ELSE NULL
	END                                                                                     AS vdr_alt_enc_id_qual,
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
	        CAST(EXTRACT_DATE(vis.admit_date_id, '%Y%m%d') AS DATE),
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
	/* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(vis.admit_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(vis.discharge_date_id, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_end_dt,
	/* enc_prov_npi */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_trx.npi,
                    dstf_dis.npi,
                    dfcl_adm.facility_npi,
                    dfcl_dis.facility_npi
                )[prov_explode.idx] IS NOT NULL
            THEN CLEAN_UP_NPI_CODE
                    (
                        ARRAY
                        (
                            dfcl.facility_npi,
                            dstf_pcp.npi,
                            dstf_trx.npi,
                            dstf_dis.npi,
                            dfcl_adm.facility_npi,
                            dfcl_dis.facility_npi
                        )[prov_explode.idx]
                    )
	    ELSE NULL
	END                                                                                     AS enc_prov_npi,
	/* enc_prov_qual */
	CASE
	    WHEN ARRAY
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_trx.npi,
                    dstf_dis.npi,
                    dfcl_adm.facility_npi,
                    dfcl_dis.facility_npi
                )[prov_explode.idx] IS NOT NULL
            THEN ARRAY
                    (
                        'RENDERING_FACILITY',
                        'PRIMARY_CARE_PHYSICIAN',
                        'TRANSFER_PROVIDER',
                        'DISCHARGE_PROVIDER',
                        'ADMIT_FROM_FACILITY',
                        'DISCHARGE_TO_FACILITY'
                    )[prov_explode.idx]
	    ELSE NULL
	END                                                                                     AS enc_prov_qual,
	/* enc_prov_alt_speclty_id */
    ARRAY
        (
            NULL,
            dstf_pcp.profession_type,
            dstf_trx.profession_type,
            dstf_dis.profession_type,
            NULL,
            NULL
        )[prov_explode.idx]                                                                 AS enc_prov_alt_speclty_id,
	/* enc_prov_alt_speclty_id_qual */
	CASE
	    WHEN ARRAY
                (
                    NULL,
                    dstf_pcp.profession_type,
                    dstf_trx.profession_type,
                    dstf_dis.profession_type,
                    NULL,
                    NULL
                )[prov_explode.idx] IS NOT NULL 
	        THEN 'PROFESSION_TYPE'
	    ELSE NULL
	END			    																		AS enc_prov_alt_speclty_id_qual,
	/* enc_prov_frst_nm */
    ARRAY
        (
            NULL,
            dstf_pcp.person_name,
            dstf_trx.person_name,
            dstf_dis.person_name,
            NULL,
            NULL
        )[prov_explode.idx]                                                                 AS enc_prov_frst_nm,
    /* enc_prov_fclty_nm */
    ARRAY
        (
            dfcl.facility_name,
            dfcl.facility_name,
            dfcl.facility_name,
            dfcl.facility_name,
            dfcl_adm.facility_name,
            dfcl_dis.facility_name
        )[prov_explode.idx]                                                                 AS enc_prov_fclty_nm,
    /* enc_prov_addr_1_txt */
    ARRAY
        (
            dfcl.address1,
            dfcl.address1,
            dfcl.address1,
            dfcl.address1,
            dfcl_adm.address1,
            dfcl_dis.address1
        )[prov_explode.idx]                                                                 AS enc_prov_addr_1_txt,
    /* enc_prov_addr_2_txt */
    ARRAY
        (
            dfcl.address2,
            dfcl.address2,
            dfcl.address2,
            dfcl.address2,
            dfcl_adm.address2,
            dfcl_dis.address2
        )[prov_explode.idx]                                                                 AS enc_prov_addr_2_txt,
    /* enc_prov_state_cd */
    VALIDATE_STATE_CODE
        (
            ARRAY
                (
                    dfcl.prov_state,
                    dfcl.prov_state,
                    dfcl.prov_state,
                    dfcl.prov_state,
                    dfcl_adm.prov_state,
                    dfcl_dis.prov_state
                )[prov_explode.idx]
        )                                                                                   AS enc_prov_state_cd,
    /* enc_prov_zip_cd */
    ARRAY
        (
            dfcl.postal_zip_code,
            dfcl.postal_zip_code,
            dfcl.postal_zip_code,
            dfcl.postal_zip_code,
            dfcl_adm.postal_zip_code,
            dfcl_dis.postal_zip_code
        )[prov_explode.idx]                                                                 AS enc_prov_zip_cd,
    CAST(NULL AS STRING)                                                                    AS enc_typ_cd,
    /* enc_grp_txt */
    CASE
        WHEN vis.length_of_stay IS NULL
            THEN NULL
        ELSE CONCAT('LENGTH_OF_STAY: ', vis.length_of_stay)
    END                                                                                     AS enc_grp_txt,
	'fact_visit'																		    AS prmy_src_tbl_nm,
	'156'																			        AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(vis.admit_date_id, '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(vis.admit_date_id, 1, 4), '-',
	                SUBSTR(vis.admit_date_id, 5, 2)
                )
	END																					    AS part_mth
 FROM vis
 LEFT OUTER JOIN dfcl
              ON vis.facility_id = dfcl.facility_id
             AND COALESCE(vis.facility_id, '0') <> '0'
 LEFT OUTER JOIN dclt
              ON vis.client_id = dclt.client_id
             AND COALESCE(vis.client_id, '0') <> '0'
 LEFT OUTER JOIN matching_payload pay
              ON dclt.resident_id = pay.personid
             AND COALESCE(dclt.resident_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_pcp
              ON dclt.primary_physician_id = dstf_pcp.staff_id
             AND COALESCE(dclt.primary_physician_id, '0') <> '0'
 LEFT OUTER JOIN dorg
              ON vis.organization_id = dorg.organization_id
             AND COALESCE(vis.organization_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_trx
              ON vis.transfer_staff_id = dstf_trx.staff_id
             AND COALESCE(vis.transfer_staff_id, '0') <> '0'
 LEFT OUTER JOIN dstf dstf_dis
              ON vis.discharge_staff_id = dstf_dis.staff_id
             AND COALESCE(vis.discharge_staff_id, '0') <> '0'
 LEFT OUTER JOIN dfcl dfcl_adm
              ON vis.admit_from_ext_facility_id = dfcl_adm.facility_id
             AND COALESCE(vis.admit_from_ext_facility_id, '0') <> '0'
 LEFT OUTER JOIN dfcl dfcl_dis
              ON vis.discharge_to_ext_facility_id = dfcl_dis.facility_id
             AND COALESCE(vis.discharge_to_ext_facility_id, '0') <> '0'
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5)) AS idx) AS prov_explode
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
  AND TRIM(UPPER(COALESCE(vis.client_id, ''))) <> 'CLIENT_ID'
  /* Provider selection. */
  /* Retrieve where the source NPI is populated, */
  /* or all of the NPIs are empty and this is the first one. */
  AND 
    (
        ARRAY
            (
                dfcl.facility_npi,
                dstf_pcp.npi,
                dstf_trx.npi,
                dstf_dis.npi,
                dfcl_adm.facility_npi,
                dfcl_dis.facility_npi
            )[prov_explode.idx] IS NOT NULL
    OR
        (
            COALESCE
                (
                    dfcl.facility_npi,
                    dstf_pcp.npi,
                    dstf_trx.npi,
                    dstf_dis.npi,
                    dfcl_adm.facility_npi,
                    dfcl_dis.facility_npi
                ) IS NULL
        AND idx = 0
        )
    )
