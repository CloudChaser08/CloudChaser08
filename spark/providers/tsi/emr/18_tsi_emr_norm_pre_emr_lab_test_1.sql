SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_lab_test_id
    --------------------------------------------------------------------------------------------------
--    CONCAT('241_',lab_rad.order_test_id, '_', lab_rad.order_specimen_source)                AS hv_lab_test_id,
    CONCAT('241_',lab_rad.order_test_id)                                                    AS hv_lab_test_id, -- Removed  specimen source. Returned NULL in the Concatenation
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(lab_rad.input_file_name, '/')[SIZE(SPLIT(lab_rad.input_file_name, '/')) - 1]              
                                                                                            AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	lab_rad.lab_rad_order_id                                                                AS vdr_lab_ord_id,
	'LAB_RAD_ORDER_ID'                                                                      AS vdr_lab_ord_id_qual,

    CASE
        WHEN lab_rad.order_specimen_source IS NOT NULL THEN CONCAT(lab_rad.order_test_id, '_', lab_rad.order_specimen_source)   
        ELSE lab_rad.order_test_id 
    END                                                                                     AS vdr_lab_test_id,
    
    CASE
        WHEN lab_rad.order_test_id IS NOT NULL THEN 'ORDER_TEST_ID'
    END                                     AS vdr_lab_test_id_qual,
    
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN pay.hvid           IS NOT NULL THEN pay.hvid
        WHEN pay.personid       IS NOT NULL THEN CONCAT('241_' , pay.personid)
        ELSE NULL 
    END																			            AS hvid,
    COALESCE(pln.date_of_birth, pay.yearofbirth)                                            AS ptnt_birth_yr,
    CASE 
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip), 1,3))                        AS ptnt_zip3_cd,
    CONCAT('241_', lab_rad.encounter_id)                                                    AS hv_enc_id,

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(lab_rad.order_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS lab_ord_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(lab_rad.specimen_collected_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS lab_test_smpl_collctn_dt,

    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS lab_test_prov_npi,
    
    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS lab_test_prov_qual,
 
    lab_rad.ordering_provider_id                                                            AS lab_test_prov_vdr_id,

    CASE
        WHEN lab_rad.ordering_provider_id IS NOT NULL THEN 'ORDERING_PROVIDER_ID'
    END                                                                                     AS lab_test_prov_vdr_id_qual,
    
    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS lab_test_prov_mdcr_speclty_cd,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', NULL)
            THEN COALESCE(prov.provider_type, prov.provider_local_specialty_name)
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name      IN ('Provider local specialty name', NULL)
            THEN prov.provider_type    
        WHEN prov.provider_type                         IN ('Provider Type', NULL)
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', NULL)
            THEN prov.provider_local_specialty_name
        ELSE NULL
    END                                                                                     AS lab_test_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', NULL)
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS lab_test_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS lab_test_prov_zip_cd,

    rslt.loinc                                                                              AS lab_test_loinc_cd,
    rslt.test_name                                                                          AS lab_result_nm,
    rslt.result_value                                                                       AS lab_result_msrmt,
    rslt.result_units_local                                                                 AS lab_result_uom,
    rslt.reference_range                                                                    AS lab_result_ref_rng_txt,
    lab_rad.order_status                                                                    AS lab_ord_stat_cd,
    'ORDER_STATUS'                                                                          AS lab_ord_stat_cd_qual,
    rslt.result_status                                                                      AS lab_test_stat_cd,
    'TEST_STATUS'                                                                           AS lab_test_stat_cd_qual,
    CONCAT
        (
        'ORDER_TYPE_CLASSIFICATION: ', lab_rad.order_type_classification, ' | ', 
        'DELETED_Y_N: ', lab_rad.deleted_y_n, ' | ', 
        'ORDER_LOCAL_CODE : ', lab_rad.order_code_local, ' | ', 
        'ORDER_NAME_LOCAL: ', lab_rad.order_name_local
        )                                                                                   AS lab_test_grp_txt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR (lab_rad.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'lab_rad'                                                                               AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------

	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM    lab_rad_orders lab_rad
LEFT OUTER JOIN results rslt
                ON COALESCE(lab_rad.order_test_id, 'NULL') = COALESCE(rslt.order_test_id, 'empty')
LEFT OUTER JOIN encounters enc
                ON lab_rad.encounter_id = enc.encounter_id
LEFT OUTER JOIN provider prov
                ON lab_rad.ordering_provider_id = prov.provider_id 
LEFT OUTER JOIN plainout pln
                ON lab_rad.patient_id = pln.person_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey 
-- Remove header records
WHERE lab_rad.patient_id <> 'PatientID'
-- LIMIT 10
