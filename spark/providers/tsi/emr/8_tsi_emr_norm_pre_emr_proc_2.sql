SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_proc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', vac.vaccine_id)  														AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'12'                                                                                    AS mdl_vrsn_num,
    SPLIT(vac.input_file_name, '/')[SIZE(SPLIT(vac.input_file_name, '/')) - 1]              AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CAST(NULL AS STRING)                                                                    AS vdr_org_id,
	vac.vaccine_id  																		AS vdr_proc_id,
	CASE 
	    WHEN vac.vaccine_id IS NOT NULL THEN 'VACCINE_ID' 
	    ELSE NULL 
	END																		                AS vdr_proc_id_qual,
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
        WHEN pln.sex IN ('F', 'M', 'U', NULL) THEN pln.sex 
        ELSE 'U' 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_', vac.encounter_id)                                                        AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vac.vaccine_administered_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS proc_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS proc_prov_npi,

    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS proc_prov_qual,

    CASE
        WHEN ARRAY  (    
                    vac.adminstering_provider_id,
                    vac.documenting_provider_id
                    )[prov_explode.n] IS NULL       THEN NULL
        ELSE ARRAY (    
                    vac.adminstering_provider_id,
                    vac.documenting_provider_id
                    )[prov_explode.n]                   
    END                                                                                     AS proc_prov_vdr_id,
    
    CASE
        WHEN prov_explode.n = 0     THEN 'ADMINISTERING_PROVIDER_ID'
        WHEN prov_explode.n = 1     THEN 'DOCUMENTING_PROVIDER_ID'
        ELSE NULL
    END                                                                                     AS proc_prov_vdr_id_qual,

    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS proc_prov_mdcr_speclty_cd,


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
    END                                                                                     AS proc_prov_alt_speclty_id,

    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', NULL)
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS proc_prov_alt_speclty_id_qual,

    CAST(NULL AS STRING)                                                                    AS proc_prov_frst_nm,
    CAST(NULL AS STRING)                                                                    AS proc_prov_last_nm,
    CAST(NULL AS STRING)                                                                    AS proc_prov_fclty_nm,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS proc_prov_zip_cd,
    UPPER(vac.vaccine_cpt_code)                                                             AS proc_cd,
    'CPT'                                                                                   AS proc_cd_qual,
    CAST(NULL AS STRING)                                                                    AS proc_cd_1_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_2_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_3_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_4_modfr,

    CASE
        WHEN UPPER(vac.vaccine_ndc) IN ('VACCINE NDC') THEN NULL
        WHEN vac.vaccine_ndc IS NOT NULL THEN vac.vaccine_ndc
        ELSE NULL
    END                                                                                     AS proc_ndc,

    CAST(NULL AS STRING)                                                                    AS proc_unit_qty,    
    CAST(NULL AS STRING)                                                                    AS proc_typ_cd,    
    CAST(NULL AS STRING)                                                                    AS proc_typ_cd_qual,    

    CASE
        WHEN vac.vaccine_route_of_administration IN ('Vaccine Route of Administration') THEN NULL
        WHEN vac.vaccine_route_of_administration IS NOT NULL THEN vac.vaccine_route_of_administration
        ELSE NULL
    END                                                                                     AS proc_admin_rte_cd,

    CONCAT
        (
            'VACCINE_CVX_CODE: ', vac.vaccine_cvx_code, ' | ', 
            'VACCINE_BRAND_NAME: ', vac.vaccine_brand_name
        )                                                                                   AS proc_grp_txt,
    CAST(NULL AS STRING)                                                                    AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(vac.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'vaccines'                                                                              AS prmy_src_tbl_nm,
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

FROM    vaccines vac
LEFT OUTER JOIN encounters enc
                ON enc.encounter_id = vac.encounter_id
LEFT OUTER JOIN provider prov
                ON prov.provider_id = enc.encounter_rendering_provider_id -- Check with Jess to ensure we should use enc.encounter_rendering_provider_id vs. enc.encounter_billing_provider_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = vac.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey
                
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) prov_explode
WHERE
----------- Provider code explosion
    (
        ARRAY
            (
                vac.adminstering_provider_id,
                vac.documenting_provider_id
            )[prov_explode.n] IS NOT NULL
    )
-- Remove header records
    AND vac.patient_id <> 'PatientID'
-- LIMIT 10
