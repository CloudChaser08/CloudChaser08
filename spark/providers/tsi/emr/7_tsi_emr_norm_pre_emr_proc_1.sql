SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', proc.procedure_order_id)                                                 AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'12'                                                                                    AS mdl_vrsn_num,
    SPLIT(proc.input_file_name, '/')[SIZE(SPLIT(proc.input_file_name, '/')) - 1]            AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CAST(NULL AS STRING)                                                                    AS vdr_org_id,
	proc.procedure_order_id                                                                 AS vdr_proc_id,
	'PROCEDURE_ID'                                                                          AS vdr_proc_id_qual,
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
    CONCAT('241_',proc.encounter_id)                                                        AS hv_enc_id,
    CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(proc.procedure_date_time, 1, 10 ), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)

	    )																					AS proc_dt,

    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS proc_prov_npi,

    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS proc_prov_qual,

    CASE
        WHEN ARRAY (    
                    proc.procedure_ordering_provider_id,
                    proc.procedure_performing_provider_id,
                    proc.procedure_billing_provider_id
                    )[prov_explode.n] IS NULL       THEN NULL
        ELSE ARRAY (    
                    proc.procedure_ordering_provider_id,
                    proc.procedure_performing_provider_id,
                    proc.procedure_billing_provider_id
                    )[prov_explode.n]                   
    END                                                                                     AS proc_prov_vdr_id,
    
    CASE
        WHEN prov_explode.n = 0     THEN 'ORDERING_PROVIDER_ID'
        WHEN prov_explode.n = 1     THEN 'PERFORMING_PROVIDER_ID'
        WHEN prov_explode.n = 2     THEN 'BILLING_PROVIDER_ID'
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
    CLEAN_UP_PROCEDURE_CODE(proc.procedure_standard_code)                                   AS proc_cd,

    CASE
        WHEN proc.procedure_type IN ('Procedure Type') THEN NULL
        ELSE proc.procedure_type
    END                                                                                     AS proc_cd_qual,

    SUBSTR(UPPER(proc.procedure_cpt_modifier), 1, 2)                                        AS proc_cd_1_modfr,
    SUBSTR(UPPER(proc.procedure_cpt_modifier2), 1, 2)                                       AS proc_cd_2_modfr,
    SUBSTR(UPPER(proc.procedure_cpt_modifier3), 1, 2)                                       AS proc_cd_3_modfr,
    SUBSTR(UPPER(proc.procedure_cpt_modifier4), 1, 2)                                       AS proc_cd_4_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_ndc,
    proc.quantity                                                                           AS proc_unit_qty,
    CAST(NULL AS STRING)                                                                    AS proc_typ_cd,
    CAST(NULL AS STRING)                                                                    AS proc_typ_cd_qual,
    CAST(NULL AS STRING)                                                                    AS proc_admin_rte_cd,
    CONCAT
        (
            'PROCEDURE_LOCAL_CODE: ', proc.procedure_local_code, ' | ', 
            'DELETED_Y_N: ', proc.deleted_y_n
        )                                                                                   AS proc_grp_txt,
    CAST(NULL AS STRING)                                                                    AS data_src_cd,
    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(proc.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'procedures'                                                                            AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
    
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------

	CASE 
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time,       1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    ) IS NULL
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.encounter_date_time, 1, 7)
	END																					    AS part_mth

FROM    procedures proc
LEFT OUTER JOIN encounters enc
            ON  enc.encounter_id = proc.encounter_id
LEFT OUTER JOIN provider prov
--            ON  prov.provider_id = enc.encounter_rendering_provider_id -- Check with Jess to ensure we should use enc.encounter_rendering_provider_id vs. enc.encounter_billing_provider_id
            ON  prov.provider_id = proc.procedure_performing_provider_id
LEFT OUTER JOIN plainout pln
            ON pln.person_id = proc.patient_id
LEFT OUTER JOIN matching_payload pay
            ON pay.hvjoinkey = pln.hvjoinkey

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2)) AS n) prov_explode
WHERE
----------- Provider code explosion
    (
        ARRAY
            (
                proc.procedure_ordering_provider_id,
                proc.procedure_performing_provider_id,
                proc.procedure_billing_provider_id
            )[prov_explode.n] IS NOT NULL
    )

-- Remove header records
    AND proc.patient_id <> 'PatientID'
