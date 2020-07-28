SELECT
    test.record_id
    ,test.claim_id
    ,test.hvid
    ,test.created
    ,test.model_version
    ,test.data_set
    ,test.data_feed
    ,test.data_vendor
    ,test.source_version
    ,test.patient_gender
    ,test.patient_age
    ,test.patient_year_of_birth
    ,test.patient_zip3
    ,test.patient_state
    ,test.date_service
    ,test.date_specimen
    ,test.date_report
    ,test.time_report
    ,test.loinc_code
    ,test.lab_id
    ,test.test_id
    ,test.test_number
    ,test.test_battery_local_id
    ,test.test_battery_std_id
    ,test.test_battery_name
    ,test.test_ordered_local_id
    ,test.test_ordered_std_id
    ,test.test_ordered_name
    ,test.result_id
    --------------------------------------------------
    ---- Merge with Quest Result
    --------------------------------------------------
    ,COALESCE(rslt.result_value_a, test.result) AS result
    ,test.result_name  AS result_name
    ,COALESCE(rslt.units, test.result_unit_of_measure) AS result_unit_of_measure
    ,test.result_desc AS result_desc
    ,COALESCE(rslt.hipaa_comment, test.result_comments) AS result_comments
    /* ref_range */
    ,COALESCE
    (
      COALESCE
      (
      	CONCAT(rslt.ref_range_low, ' - ', rslt.ref_range_high),
      	rslt.ref_range_alpha
      ),
      test.ref_range
    ) AS ref_range
    ,COALESCE(rslt.abnormal_ind, test.abnormal_flag) AS abnormal_flag
    ,COALESCE(rslt.fasting_indicator, test.fasting_status) AS fasting_status
    ------------------------------------------------
    ,test.diagnosis_code
    ,test.diagnosis_code_qual
    ,test.diagnosis_code_priority
    ,test.procedure_code
    ,test.procedure_code_qual
    ,test.procedure_modifier_1
    ,test.procedure_modifier_2
    ,test.procedure_modifier_3
    ,test.procedure_modifier_4
    ,test.lab_npi
    ,test.ordering_npi
    ,test.payer_id
    ,test.payer_id_qual
    ,test.payer_name
    ,test.payer_parent_name
    ,test.payer_org_name
    ,test.payer_plan_id
    ,test.payer_plan_name
    ------------------------------------------------
    ,COALESCE(rslt.insurance_billing_type, test.payer_type) AS payer_type
    ------------------------------------------------
    ,test.lab_other_id
    ,test.lab_other_qual
    ,test.lab_address_1
    ,test.lab_address_2
    ,test.lab_city
    ,test.lab_state
    ,test.lab_zip
    ,test.ordering_other_id
    ,test.ordering_other_qual
    ,test.ordering_name
    ,test.ordering_market_type
    ,test.ordering_specialty
    ,test.ordering_vendor_id
    ,test.ordering_tax_id
    ,test.ordering_dea_id
    ,test.ordering_ssn
    ,test.ordering_state_license
    ,test.ordering_upin
    ,test.ordering_commercial_id
    ,test.ordering_address_1
    ,test.ordering_address_2
    ,test.ordering_city
    ,test.ordering_state
    ,test.ordering_zip
    ,test.medication_generic_name
    ,test.medication_dose
    ,test.logical_delete_reason
    ,test.vendor_record_id
    ,CAST (
        CASE WHEN (
            test.procedure_code      IN ('87635','86328','86769','U0003','U0004','U0001','U0002')
            OR test.test_ordered_std_id IN ('39433X','0039433','39444','39444X','39433','0039444','39504','39504X','0039504','0039728','39448','TH698','TH680','TH714','TH99')
            OR test.loinc_code          IN ('945006','414581','753251','943092','945634')
            OR lower(test.test_ordered_name) LIKE '%covid%19%'
            OR lower(test.result_name)       LIKE '%covid%19%'
            OR lower(test.test_ordered_name) LIKE '%sars%cov%2%'
            OR lower(test.result_name)       LIKE '%sars%cov%2%'
            ) THEN
                CASE WHEN test.part_provider = 'quest' THEN
                    CASE WHEN COALESCE(test.test_ordered_local_id, test.claim_id ) = COALESCE(rslt.local_order_code , rslt.claim_id) THEN 1 ELSE 0 END
                ELSE 1 END
        ELSE 0 END AS INT) AS covid19_ind
    ,test.claim_bucket_id
    ,test.part_provider
    ,test.part_mth
FROM
    _temp_lab_tests test
    LEFT OUTER JOIN
        _temp_lab_results rslt
            ON test.part_provider = rslt.part_provider
            AND test.part_mth = rslt.part_mth
            AND test.claim_bucket_id = rslt.claim_bucket_id
            AND test.claim_id = rslt.claim_id
            AND COALESCE(test.test_ordered_local_id, test.claim_id ) = COALESCE(rslt.local_order_code , rslt.claim_id)
                ----------------- If both fields are not populated skip the check----------------  -- AND a.test_ordered_std_id=c.standard_order_code
            AND CASE WHEN (test.test_ordered_std_id IS NOT NULL OR LENGTH(TRIM(COALESCE(test.test_ordered_std_id,''))) > 0  )
                    AND (rslt.standard_order_code IS NOT NULL OR LENGTH(TRIM(COALESCE(rslt.standard_order_code,''))) > 0  )
                        THEN  test.test_ordered_std_id   ELSE 1  END
                = CASE WHEN (test.test_ordered_std_id IS NOT NULL OR LENGTH(TRIM(COALESCE(test.test_ordered_std_id,''))) > 0  )
                    AND (rslt.standard_order_code IS NOT NULL OR LENGTH(TRIM(COALESCE(rslt.standard_order_code,''))) > 0  )
                        THEN rslt.standard_order_code   ELSE 1  END


