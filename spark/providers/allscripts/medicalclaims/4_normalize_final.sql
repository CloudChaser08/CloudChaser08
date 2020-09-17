SELECT
    MONOTONICALLY_INCREASING_ID()                                                       AS record_id
    ,claim_id
    ,hvid
    ,CURRENT_DATE()                                                                     AS created
	,'06'                                                                               AS model_version
    ,CONCAT('HV_CLAIMS_', replace('{VDR_FILE_DT}','-', ''))                             AS data_set
	,'26'                                                                               AS data_feed
	,'35'                                                                               AS data_vendor
    ,source_version
    ,NULL                                                                               AS vendor_org_id
    ,CLEAN_UP_GENDER (patient_gender)                                                   AS patient_gender
    ,NULL                                                                               AS patient_age
    ,CAP_YEAR_OF_BIRTH (NULL, date_service, patient_year_of_birth)                      AS patient_year_of_birth
    ,MASK_ZIP_CODE(patient_zip3)                                                        AS patient_zip3
    ,VALIDATE_STATE_CODE(patient_state)                                                 AS patient_state
    ,claim_type
    ,date_received
	,CAP_DATE (
            date_service,
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}'  AS DATE)
        )                                                                               AS date_service
	,CAP_DATE (
            date_service_end,
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                               AS date_service_end
    ,NULL AS inst_date_admitted
    ,NULL AS inst_date_discharged
    ,NULL AS inst_admit_type_std_id
    ,NULL AS inst_admit_type_vendor_id
    ,NULL AS inst_admit_type_vendor_desc
    ,NULL AS inst_admit_source_std_id
    ,NULL AS inst_admit_source_vendor_id
    ,NULL AS inst_admit_source_vendor_desc
    ,NULL AS inst_discharge_status_std_id
    ,NULL AS inst_discharge_status_vendor_id
    ,NULL AS inst_discharge_status_vendor_desc
    ,NULL AS inst_type_of_bill_std_id
    ,NULL AS inst_type_of_bill_vendor_id
    ,NULL AS inst_type_of_bill_vendor_desc
    ,NULL AS inst_drg_std_id
    ,NULL AS inst_drg_vendor_id
    ,NULL AS inst_drg_vendor_desc
    ,place_of_service_std_id
    ,NULL AS place_of_service_vendor_id
    ,NULL AS place_of_service_vendor_desc
    ,service_line_number
    ,CLEAN_UP_DIAGNOSIS_CODE(
        diagnosis_code, NULL, date_service
        )                                                                               AS diagnosis_code
--    ,diagnosis_code
    ,NULL AS diagnosis_code_qual
    ,diagnosis_priority
    ,NULL AS admit_diagnosis_ind
    ,CLEAN_UP_PROCEDURE_CODE(procedure_code)                                            AS procedure_code
    ,NULL AS procedure_code_qual
    ,NULL AS principal_proc_ind
    ,procedure_units_billed
    ,NULL AS procedure_units_paid
    ,procedure_modifier_1
    ,procedure_modifier_2
    ,procedure_modifier_3
    ,procedure_modifier_4
    ,NULL AS revenue_code
    ,CLEAN_UP_NDC_CODE(ndc_code)                                                        AS ndc_code
    ,medical_coverage_type
    ,line_charge
    ,NULL AS line_allowed
    ,total_charge
    ,NULL AS total_allowed
    ,filter_due_to_place_of_service(prov_rendering_npi, place_of_service_std_id)        AS prov_rendering_npi
    ,filter_due_to_place_of_service(prov_billing_npi, place_of_service_std_id)          AS prov_billing_npi
    ,filter_due_to_place_of_service(prov_referring_npi, place_of_service_std_id)        AS prov_referring_npi
    ,filter_due_to_place_of_service(prov_facility_npi, place_of_service_std_id)         AS prov_facility_npi
    ,NULL AS payer_vendor_id
    ,payer_name
    ,NULL AS payer_parent_name
    ,NULL AS payer_org_name
    ,payer_plan_id
    ,NULL AS payer_plan_name
    ,payer_type
    ,NULL AS prov_rendering_vendor_id
    ,filter_due_to_place_of_service(prov_rendering_tax_id, place_of_service_std_id)     AS prov_rendering_tax_id
    ,NULL AS prov_rendering_dea_id
    ,filter_due_to_place_of_service(prov_rendering_ssn, place_of_service_std_id)        AS prov_rendering_ssn
    ,NULL AS prov_rendering_state_license
    ,NULL AS prov_rendering_upin
    ,NULL AS prov_rendering_commercial_id
    ,NULL AS prov_rendering_name_1
    ,filter_due_to_place_of_service(prov_rendering_name_2, place_of_service_std_id)     AS prov_rendering_name_2
    ,NULL AS prov_rendering_address_1
    ,NULL AS prov_rendering_address_2
    ,NULL AS prov_rendering_city
    ,NULL AS prov_rendering_state
    ,NULL AS prov_rendering_zip
    ,prov_rendering_std_taxonomy
    ,NULL AS prov_rendering_vendor_specialty
    ,NULL AS prov_billing_vendor_id
    ,filter_due_to_place_of_service(prov_billing_tax_id, place_of_service_std_id)       AS prov_billing_tax_id
    ,NULL AS prov_billing_dea_id
    ,filter_due_to_place_of_service(prov_billing_ssn, place_of_service_std_id)          AS prov_billing_ssn
    ,NULL AS prov_billing_state_license
    ,NULL AS prov_billing_upin
    ,NULL AS prov_billing_commercial_id
    ,filter_due_to_place_of_service(prov_billing_name_1, place_of_service_std_id)       AS prov_billing_name_1
    ,filter_due_to_place_of_service(prov_billing_name_2, place_of_service_std_id)       AS prov_billing_name_2
    ,filter_due_to_place_of_service(prov_billing_address_1, place_of_service_std_id)    AS prov_billing_address_1
    ,filter_due_to_place_of_service(prov_billing_address_2, place_of_service_std_id)    AS prov_billing_address_2
    ,filter_due_to_place_of_service(prov_billing_city, place_of_service_std_id)         AS prov_billing_city
    ,filter_due_to_place_of_service(prov_billing_state, place_of_service_std_id)        AS prov_billing_state
    ,filter_due_to_place_of_service(prov_billing_zip, place_of_service_std_id)          AS prov_billing_zip
    ,prov_billing_std_taxonomy
    ,NULL AS prov_billing_vendor_specialty
    ,NULL AS prov_referring_vendor_id
    ,filter_due_to_place_of_service(prov_referring_tax_id, place_of_service_std_id)     AS prov_referring_tax_id
    ,NULL AS prov_referring_dea_id
    ,filter_due_to_place_of_service(prov_referring_ssn, place_of_service_std_id)        AS prov_referring_ssn
    ,NULL AS prov_referring_state_license
    ,NULL AS prov_referring_upin
    ,NULL AS prov_referring_commercial_id
    ,NULL AS prov_referring_name_1
    ,filter_due_to_place_of_service(prov_referring_name_2, place_of_service_std_id)     AS prov_referring_name_2
    ,NULL AS prov_referring_address_1
    ,NULL AS prov_referring_address_2
    ,NULL AS prov_referring_city
    ,NULL AS prov_referring_state
    ,NULL AS prov_referring_zip
    ,prov_referring_std_taxonomy
    ,NULL AS prov_referring_vendor_specialty
    ,NULL AS prov_facility_vendor_id
    ,filter_due_to_place_of_service(prov_facility_tax_id, place_of_service_std_id)      AS prov_facility_tax_id
    ,NULL AS prov_facility_dea_id
    ,filter_due_to_place_of_service(prov_facility_ssn, place_of_service_std_id)         AS prov_facility_ssn
    ,NULL AS prov_facility_state_license
    ,NULL AS prov_facility_upin
    ,NULL AS prov_facility_commercial_id
    ,filter_due_to_place_of_service(prov_facility_name_1, place_of_service_std_id)      AS prov_facility_name_1
    ,NULL AS prov_facility_name_2
    ,filter_due_to_place_of_service(prov_facility_address_1, place_of_service_std_id)   AS prov_facility_address_1
    ,filter_due_to_place_of_service(prov_facility_address_2, place_of_service_std_id)   AS prov_facility_address_2
    ,filter_due_to_place_of_service(prov_facility_city, place_of_service_std_id)        AS prov_facility_city
    ,filter_due_to_place_of_service(prov_facility_state, place_of_service_std_id)       AS prov_facility_state
    ,filter_due_to_place_of_service(prov_facility_zip, place_of_service_std_id)         AS prov_facility_zip
    ,NULL AS prov_facility_std_taxonomy
    ,NULL AS prov_facility_vendor_specialty
    ,cob_payer_vendor_id_1
    ,cob_payer_seq_code_1
    ,NULL AS cob_payer_hpid_1
    ,cob_payer_claim_filing_ind_code_1
    ,cob_ins_type_code_1
    ,cob_payer_vendor_id_2
    ,cob_payer_seq_code_2
    ,NULL AS cob_payer_hpid_2
    ,cob_payer_claim_filing_ind_code_2
    ,cob_ins_type_code_2
    ,NULL AS vendor_test_id
    ,NULL AS vendor_test_name
    ,NULL AS claim_transaction_date
    ,NULL AS claim_transaction_date_qual
    ,NULL AS claim_transaction_amount
    ,NULL AS claim_transaction_amount_qual
    ,medical_claim_link_text
    ,NULL AS logical_delete_reason
	,'allscripts'                                                                       AS part_provider
    /* part_best_date */
	,CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            date_service,
                                            CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(CAST(date_service AS STRING), 1, 7)
	END                                     AS part_best_date
FROM
    (
        SELECT * FROM normalize_1
        UNION
        SELECT * FROM normalize_2
    ) z
