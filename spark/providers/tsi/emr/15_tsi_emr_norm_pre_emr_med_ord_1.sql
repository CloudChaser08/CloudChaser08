SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', med_ord.medication_order_id)												AS hv_medctn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(med_ord.input_file_name, '/')[SIZE(SPLIT(med_ord.input_file_name, '/')) - 1]      AS data_set_nm,
	1801                                                                                    AS hvm_vdr_id,
	241                                                                                     AS hvm_vdr_feed_id,
    med_ord.medication_order_id                                                             AS vdr_medctn_ord_id,
	CASE 
	    WHEN med_ord.medication_order_id IS NOT NULL THEN 'MEDICATION_ORDER_ID' 
	    ELSE NULL 
	END																		                AS vdr_medctn_ord_id_qual,
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
    --------------------------------------------------------------------------------------------------
    --  enc_start_dt - Add Capping when date is available - Use CASE Logic for the dates 
    --------------------------------------------------------------------------------------------------
    CONCAT('241_', med_ord.encounter_id)                                                    AS hv_enc_id,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.encounter_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS enc_dt,
    CLEAN_UP_NPI_CODE(prov.provider_npi)                                                    AS medctn_prov_npi,

    CASE
        WHEN prov.provider_npi IS NULL THEN NULL
        ELSE 'BILLING_PROVIDER_NPI'
    END                                                                                     AS medctn_prov_qual,

    med_ord.medication_order_prescribing_physician                                          AS medctn_prov_vdr_id,
    'PRESCRIBING_PHYSICIAN'                                                                 AS medctn_prov_vdr_id_qual,
    CASE
        WHEN prov.provider_local_specialty_code NOT IN ('Provider local specialty code') 
            THEN prov.provider_local_specialty_code
    END                                                                                     AS medctn_prov_mdcr_speclty_cd,

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
    END                                                                                     AS medctn_prov_alt_speclty_id,
    CASE
        WHEN prov.provider_type                     NOT IN ('Provider Type', NULL)
            OR prov.provider_local_specialty_name   NOT IN ('Provider local specialty name', NULL)
            THEN 'SPECIALTY_NAME'
    END                                                                                     AS medctn_prov_alt_speclty_id_qual,
    MASK_ZIP_CODE(prov.zip_code)                                                            AS medctn_prov_zip_cd,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(med_ord.med_order_start_date,1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS medctn_start_dt,
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(med_ord.med_order_end_date,1, 8), '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )			                														AS medctn_end_dt,


    CLEAN_UP_NDC_CODE(med_ord.medication_order_ndc)                                         AS medctn_ndc,
    med_ord.med_order_dispence_quantity                                                     AS medctn_dispd_qty,
    med_ord.med_order_strength                                                              AS medctn_strth_txt,
    med_ord.med_order_units                                                                 AS medctn_dose_uom,

    CASE
        WHEN med_ord.medication_order_refills_authorized = 'PRN'
            THEN NULL
        ELSE med_ord.medication_order_refills_authorized
    END                                                                                     AS medctn_remng_rfll_qty,
    CASE
        WHEN UPPER(med_ord.med_order_action) = 'ELECTRONIC' THEN 'Y'
        WHEN UPPER(med_ord.med_order_action) = 'PRINT' THEN 'N'
        WHEN UPPER(med_ord.med_order_action) = 'FAX' THEN 'N'
        WHEN UPPER(med_ord.med_order_action) = 'MED ORDER ACTION' THEN 'N'
        ELSE NULL
        END                                                                                 AS medctn_elect_rx_flg,
 
    CONCAT
        (
            'DISPENSE_AS_WRITTEN_FLAG: ', med_ord.medication_order_daw, ' | ', 
            'MEDICATION_ORDER_LOCAL_CODE: ', med_ord.medication_order_local_code,  ' | ', 
            'MEDICATION_ORDER_LOCAL_NAME: ', med_ord.medication_order_local_name
        )                                                                                   AS medctn_grp_txt,

	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR (med_ord.record_create_date_time, 1, 10), '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )																					AS data_captr_dt,        
    'medication_orders'                                                                     AS prmy_src_tbl_nm,
    '241'                                                                                   AS part_hvm_vdr_feed_id,
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


FROM    medication_orders med_ord
LEFT OUTER JOIN encounters enc
            ON enc.encounter_id = med_ord.encounter_id
LEFT OUTER JOIN provider prov
            ON prov.provider_id = med_ord.medication_order_prescribing_physician
LEFT OUTER JOIN plainout pln
            ON pln.person_id = med_ord.patient_id
LEFT OUTER JOIN matching_payload pay
            ON pay.hvjoinkey = pln.hvjoinkey
-- Remove header records
WHERE med_ord.patient_id <> 'PatientID'

-- LIMIT 10
