SELECT
    MONOTONICALLY_INCREASING_ID()   AS record_id,
    quest.claim_id,
    dwlab.hvid,
    dwlab.created,
    dwlab.model_version,
    CONCAT(dwlab.data_set,'_',quest.batch_id) AS data_set,
    dwlab.data_feed,
    dwlab.data_vendor,
    dwlab.source_version,
    quest.gender              AS patient_gender,
    quest.hipaa_age           AS patient_age	,
    quest.hipaa_dob           AS patient_year_of_birth,
    quest.hipaa_zip           AS patient_zip3,
    CAST(NULL AS STRING) AS patient_state,
    to_date(from_unixtime(unix_timestamp(quest.DOS,'yyyyMMdd')))    AS date_service,
    dwlab.date_specimen,
    CAST(NULL AS DATE) AS date_report,
    CAST(NULL AS TIMESTAMP) AS time_report,
    quest.loinc_code          AS loinc_code,
    quest.lab_code            AS lab_id,
    CAST(NULL AS STRING) AS test_id,
    CAST(NULL AS STRING) AS test_number,
    CAST(NULL AS STRING) AS test_battery_local_id,
    CAST(NULL AS STRING) AS test_battery_std_id,
    CAST(NULL AS STRING) AS test_battery_name,
    quest.local_order_code    AS test_ordered_local_id	,
    quest.standard_order_code AS test_ordered_std_id,
    quest.order_name          AS test_ordered_name,
    quest.local_result_code   AS result_id,
    quest.result_value_a	  AS result,
    quest.result_name	      AS result_name,
    quest.units               AS result_unit_of_measure,
    CAST(NULL AS STRING) AS result_desc,
    quest.hipaa_comment       AS result_comments,
    CASE
        WHEN LENGTH(TRIM(quest.ref_range_alpha)) > 0
            THEN quest.ref_range_alpha
        WHEN LENGTH(TRIM(quest.ref_range_low )) > 0 AND LENGTH(TRIM(quest.ref_range_high)) > 0
            THEN CONCAT(quest.ref_range_low, ' - ', quest.ref_range_high)
    END AS ref_range,
    quest.abnormal_ind              AS abnormal_flag,
    quest.fasting_indicator         AS fasting_status,
    EXPLODE_OUTER(
        SPLIT(
            TRIM(REGEXP_REPLACE(
                    REPLACE(quest.diagnosis_code, '10^','')
                ,"\\s+", " "))
            ," ")
        )  AS diagnosis_code,
    CASE
        WHEN quest.icd_codeset_ind =  '9'  THEN  '01'
        WHEN quest.icd_codeset_ind =  '10' THEN  '02'
    END diagnosis_code_qual,
    CAST(NULL AS STRING) AS diagnosis_code_priority,
    CAST(NULL AS STRING) AS procedure_code,
    CAST(NULL AS STRING) AS procedure_code_qual,
    CAST(NULL AS STRING) AS procedure_modifier_1,
    CAST(NULL AS STRING) AS procedure_modifier_2,
    CAST(NULL AS STRING) AS procedure_modifier_3,
    CAST(NULL AS STRING) AS procedure_modifier_4,
    CAST(NULL AS STRING) AS lab_npi,
    CAST(NULL AS STRING) AS ordering_npi,
    CAST(NULL AS STRING) AS payer_id,
    CAST(NULL AS STRING) AS payer_id_qual,
    CAST(NULL AS STRING) AS payer_name,
    CAST(NULL AS STRING) AS payer_parent_name,
    CAST(NULL AS STRING) AS payer_org_name,
    CAST(NULL AS STRING) AS payer_plan_id,
    CAST(NULL AS STRING) AS payer_plan_name,
    CAST(NULL AS STRING) AS payer_type,
    CAST(NULL AS STRING) AS lab_other_id,
    CAST(NULL AS STRING) AS lab_other_qual,
    CAST(NULL AS STRING) AS lab_address_1,
    CAST(NULL AS STRING) AS lab_address_2,
    CAST(NULL AS STRING) AS lab_city,
    CAST(NULL AS STRING) AS lab_state,
    CAST(NULL AS STRING) AS lab_zip,
    CAST(NULL AS STRING) AS ordering_other_id,
    CAST(NULL AS STRING) AS ordering_other_qual,
    CAST(NULL AS STRING) AS ordering_name,
    CAST(NULL AS STRING) AS ordering_market_type,
    CAST(NULL AS STRING) AS ordering_specialty,
    CAST(NULL AS STRING) AS ordering_vendor_id,
    CAST(NULL AS STRING) AS ordering_tax_id,
    CAST(NULL AS STRING) AS ordering_dea_id,
    CAST(NULL AS STRING) AS ordering_ssn,
    CAST(NULL AS STRING) AS ordering_state_license,
    CAST(NULL AS STRING) AS ordering_upin,
    CAST(NULL AS STRING) AS ordering_commercial_id,
    CAST(NULL AS STRING) AS ordering_address_1,
    CAST(NULL AS STRING) AS ordering_address_2,
    CAST(NULL AS STRING) AS ordering_city,
    CAST(NULL AS STRING) AS ordering_state,
    CAST(NULL AS STRING) AS ordering_zip,
    CAST(NULL AS STRING) AS medication_generic_name,
    CAST(NULL AS STRING) AS medication_dose,
    CAST(NULL AS STRING) AS logical_delete_reason,
    CAST(NULL AS STRING) AS vendor_record_id,
    NVL(quest.claim_bucket_id,0) AS claim_bucket_id,
    quest.part_provider,
    quest.part_mth
FROM
    lab_collect_results2575 quest
    LEFT OUTER JOIN lab_collect_tests dwlab
        ON dwlab.part_provider = quest.part_provider
            AND dwlab.part_mth = quest.part_mth
            AND dwlab.claim_bucket_id = quest.claim_bucket_id
            AND dwlab.claim_id = quest.claim_id
            AND dwlab.patient_gender	    = quest.gender
            AND dwlab.patient_year_of_birth	= quest.hipaa_dob
            AND dwlab.patient_zip3	        = quest.hipaa_zip
            AND dwlab.date_service	        = to_date(from_unixtime(unix_timestamp(quest.DOS,'yyyyMMdd')))  -- CAST(quest.DOS AS DATE)
            AND dwlab.lab_id	            = quest.lab_code
            AND dwlab.test_ordered_local_id	= quest.local_order_code
            AND dwlab.test_ordered_std_id   = quest.standard_order_code
            AND dwlab.test_ordered_name     = quest.order_name
            AND dwlab.part_provider = 'quest'
WHERE
    quest.part_provider = 'quest'


