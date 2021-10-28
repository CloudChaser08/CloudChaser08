SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_proc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', med_surg.unique_id)														AS hv_proc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'12'                                                                                    AS mdl_vrsn_num,
    SPLIT(med_surg.input_file_name, '/')[SIZE(SPLIT(med_surg.input_file_name, '/')) - 1]    AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
	CAST(med_surg.practice_id AS STRING)                                                    AS vdr_org_id,
	med_surg.unique_id																		AS vdr_proc_id,
	CASE 
	    WHEN med_surg.unique_id IS NOT NULL THEN 'MED_SURG_HISTORY_PROCEDURE_ID' 
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
        WHEN pln.sex IN ('F', 'M', 'U') THEN pln.sex 
        ELSE NULL 
    END                                                                                     AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(UPPER(COALESCE(pln.state, pay.state)))                              AS ptnt_state_cd, 
    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
 	MASK_ZIP_CODE(SUBSTR(COALESCE(pln.zip, pay.threedigitzip),1,3))                         AS ptnt_zip3_cd,
    CONCAT('241_',med_surg.encounter_id)                                                    AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
    COALESCE
        (
            EXTRACT_DATE(med_surg.procedure_date, '%m/%Y'),    
            EXTRACT_DATE(med_surg.procedure_date, '%m/%d/%Y'), 
            EXTRACT_DATE(med_surg.procedure_date, '%Y'), 
            EXTRACT_DATE(med_surg.procedure_year, '%Y') 
        )                                                                                   AS proc_dt,
    CAST(NULL AS STRING)                                                                    AS proc_prov_npi,
    CAST(NULL AS STRING)                                                                    AS proc_prov_qual,
    CAST(NULL AS STRING)                                                                    AS proc_prov_vdr_id,
    CAST(NULL AS STRING)                                                                    AS proc_prov_vdr_id_qual,
    CAST(NULL AS STRING)                                                                    AS proc_prov_mdcr_speclty_cd,
    CAST(NULL AS STRING)                                                                    AS proc_prov_alt_speclty_id,
    CAST(NULL AS STRING)                                                                    AS proc_prov_alt_speclty_id_qual,
    med_surg.first_name_of_provider_performing_procedure                                    AS proc_prov_frst_nm,
    med_surg.last_name_of_provider_performing_procedure                                     AS proc_prov_last_nm,
    med_surg.facility_type_where_procedure_was_performed                                    AS proc_prov_fclty_nm,

    CAST(NULL AS STRING)                                                                    AS proc_prov_zip_cd,
    CAST(NULL AS STRING)                                                                    AS proc_cd,
    CAST(NULL AS STRING)                                                                    AS proc_cd_qual,
    CAST(NULL AS STRING)                                                                    AS proc_cd_1_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_2_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_3_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_cd_4_modfr,
    CAST(NULL AS STRING)                                                                    AS proc_ndc,
    CAST(NULL AS STRING)                                                                    AS proc_unit_qty,
    
    med_surg.surgical_procedure                                                             AS proc_typ_cd,
    'SURGICAL_PROCEDURE_CODE'                                                               AS proc_typ_cd_qual,
    CAST(NULL AS STRING)                                                                    AS proc_admin_rte_cd,
    med_surg.enterprise_id                                                                  AS data_src_cd,
    CAST(NULL AS STRING)                                                                    AS proc_grp_txt,

    --------------------------------------------------------------------------------------------------
    --  data_captr_dt
    --------------------------------------------------------------------------------------------------
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(med_surg.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,
    'medical_and_surgical_history'                                                          AS prmy_src_tbl_nm,
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

FROM    medical_and_surgical_history med_surg
LEFT OUTER JOIN encounters enc
                ON enc.encounter_id = med_surg.encounter_id
LEFT OUTER JOIN plainout pln
                ON pln.person_id = med_surg.patient_id
LEFT OUTER JOIN matching_payload pay
                ON pay.hvjoinkey = pln.hvjoinkey
-- Remove header records
WHERE TRIM(lower(COALESCE(med_surg.patient_id, 'empty'))) <> 'patientid'
-- LIMIT 50
