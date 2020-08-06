SELECT
    record_id
    ,claim_id
    ,hvid
    ,created
    ,model_version
    ,data_set
    ,data_feed
    ,data_vendor
    ,source_version
    ,patient_gender
    ,patient_age
    ,patient_year_of_birth
    ,patient_zip3
    ,patient_state
    ,date_service
    ,date_specimen
    ,date_report
    ,time_report
    ,loinc_code
    ,lab_id
    ,test_id
    ,test_number
    ,test_battery_local_id
    ,test_battery_std_id
    ,test_battery_name
    ,test_ordered_local_id
    ,test_ordered_std_id
    ,test_ordered_name
    ,result_id
    ,result
    ,result_name
    ,result_unit_of_measure
    ,result_desc
    ,result_comments
	,COALESCE
	    (
	        CONCAT
	            (
	                ref_range_low, ' - ',
	                ref_range_high
	            ),
	            ref_range_alpha
	    )  AS ref_range
    ,abnormal_flag
    ,fasting_status
    ,diagnosis_code
    ,diagnosis_code_qual
    ,diagnosis_code_priority
    ,procedure_code
    ,procedure_code_qual
    ,procedure_modifier_1
    ,procedure_modifier_2
    ,procedure_modifier_3
    ,procedure_modifier_4
    ,lab_npi
    ,CAST (NULL AS STRING) AS ordering_npi
    ,payer_id
    ,payer_id_qual
    ,payer_name
    ,payer_parent_name
    ,payer_org_name
    ,payer_plan_id
    ,payer_plan_name
    ,payer_type
    ,lab_other_id
    ,lab_other_qual
    ,lab_address_1
    ,lab_address_2
    ,lab_city
    ,lab_state
    ,lab_zip
    ,CAST (NULL AS STRING) AS ordering_other_id
    ,CAST (NULL AS STRING) AS ordering_other_qual
    ,CAST (NULL AS STRING) AS ordering_name
    ,CAST (NULL AS STRING) AS ordering_market_type
    ,CAST (NULL AS STRING) AS ordering_specialty
    ,CAST (NULL AS STRING) AS ordering_vendor_id
    ,CAST (NULL AS STRING) AS ordering_tax_id
    ,CAST (NULL AS STRING) AS ordering_dea_id
    ,CAST (NULL AS STRING) AS ordering_ssn
    ,CAST (NULL AS STRING) AS ordering_state_license
    ,CAST (NULL AS STRING) AS ordering_upin
    ,CAST (NULL AS STRING) AS ordering_commercial_id
    ,CAST (NULL AS STRING) AS ordering_address_1
    ,CAST (NULL AS STRING) AS ordering_address_2
    ,CAST (NULL AS STRING) AS ordering_city
    ,CAST (NULL AS STRING) AS ordering_state
    ,CAST (NULL AS STRING) AS ordering_zip
    ,medication_generic_name
    ,medication_dose
    ,logical_delete_reason
    ,vendor_record_id
    ,CAST(mod(case when claim_id is null then 0.0 else substring(claim_id,1,instr(claim_id,'_')-1) end, CAST({nbr_of_buckets} AS INT)) AS INT) as claim_bucket_id
    ,part_provider
	,CASE
	    WHEN COALESCE(part_best_date, 'NULL') NOT IN ('NULL', '0_PREDATES_HVM_HISTORY')
	        THEN SUBSTR(part_best_date, 1, 7)
	    ELSE '0_PREDATES_HVM_HISTORY'
	END AS part_mth
FROM
    dw._labtests_nb
WHERE
     part_provider = {part_provider}
     AND CASE
 	    WHEN COALESCE(part_best_date, 'NULL') NOT IN ('NULL', '0_PREDATES_HVM_HISTORY')
 	        THEN SUBSTR(part_best_date, 1, 7)
 	    ELSE '0_PREDATES_HVM_HISTORY'
 	END IN ({list_of_part_mth})