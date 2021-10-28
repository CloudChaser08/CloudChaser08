SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_prov_ord_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', gen_ord.order_id)														AS hv_prov_ord_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(gen_ord.input_file_name, '/')[SIZE(SPLIT(gen_ord.input_file_name, '/')) - 1]      AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	gen_ord.order_id																		AS vdr_prov_ord_id,
	CASE 
	    WHEN gen_ord.order_id IS NOT NULL THEN 'ORDER_ID' 
	    ELSE NULL 
	END																		                AS vdr_prov_ord_id_qual,
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
        ELSE NULL 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', gen_ord.encounter_id)                                                    AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(gen_ord.order_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS prov_ord_dt,
    gen_ord.ordering_provider_id                                                            AS prov_ord_prov_vdr_id,

    CASE
        WHEN gen_ord.ordering_provider_id IS NULL THEN NULL
        ELSE 'ORDERING_PROVIDER_ID'                                 
    END                                                                                     AS prov_ord_prov_vdr_id_qual, 

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS prov_ord_prov_mdcr_speclty_cd,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN COALESCE(prov.provider_type, prov.provider_local_specialty_name)
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name      IN ('Provider local specialty name', 'null')
            THEN prov.provider_type    
        WHEN prov.provider_type                         IN ('Provider Type', 'null')
            AND prov.provider_local_specialty_name  NOT IN ('Provider local specialty name', 'null')
            THEN prov.provider_local_specialty_name
        ELSE NULL
    END                                                                                     AS prov_ord_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', 'null')
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', 'null')
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS prov_ord_prov_alt_speclty_id_qual,
 
    MASK_ZIP_CODE(prov.zip_code)                                                            AS prov_ord_prov_zip_cd,
    
    gen_ord.order_type_classification                                                       AS prov_ord_typ_cd,
    
    CASE
        WHEN gen_ord.order_type_classification IS NULL THEN NULL
        ELSE 'ORDER_TYPE_CLASSIFICATION'
    END                                                                                     AS prov_ord_typ_cd_qual,
    
    gen_ord.order_name_local                                                                AS prov_ord_nm,
    CAST(gen_ord.order_code_local AS STRING)                                                AS prov_ord_alt_cd,
   
    CASE
        WHEN gen_ord.order_code_local IS NULL THEN NULL
        ELSE 'ORDER_CODE_LOCAL'
    END                                                                                     AS prov_ord_alt_cd_qual,
   
    CLEAN_UP_NDC_CODE(gen_ord.ndc_id)                                                       AS prov_ord_ndc,
    gen_ord.order_status                                                                    AS prov_ord_stat_nm,
    
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR ( gen_ord.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'general_orders'                                                                        AS prmy_src_tbl_nm,
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

FROM    general_orders gen_ord
LEFT OUTER JOIN encounters enc
                ON COALESCE(enc.encounter_id, 'NULL')   = COALESCE(gen_ord.encounter_id, 'empty')
LEFT OUTER JOIN provider prov
                ON  prov.provider_id = COALESCE(gen_ord.ordering_provider_id, 0) <> 0
LEFT OUTER JOIN plainout pln
                ON pln.person_id = gen_ord.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey
INNER JOIN      gen_ref_whtlst allow_list
                ON gen_ord.order_name_local = allow_list.gen_ref_nm
-- Remove header records
WHERE TRIM(lower(COALESCE(gen_ord.patient_id, 'empty'))) <> 'patientid' 
-- LIMIT 5
