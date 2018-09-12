SELECT
    CONCAT
        (
            txn.accession_number_hashed, '_',
            txn.case_number_hashed
        )                                                                               AS claim_id,
    pay.hvid                                                                            AS hvid,
    CONCAT('TestMeta_', cast(txn.vendor_file_date as string), '.txt')                   AS data_set,
    CASE WHEN UPPER(pay.gender) IN ('F', 'M') THEN UPPER(pay.gender)
        ELSE 'U'
    END                                                                                 AS patient_gender,
    pay.age                                                                             AS patient_age,
    pay.yearofbirth                                                                     AS patient_year_of_birth,
    pay.threedigitzip                                                                   AS patient_zip3,
    pay.state                                                                           AS patient_state, -- 10
    EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y', cast({min_date} as date), cast(txn.vendor_file_date as date))
                                                                                        AS date_service,
    EXTRACT_DATE(txn.specimen_collection_date, '%m/%d/%Y', cast({min_date} as date), cast(txn.vendor_file_date as date))
                                                                                        AS date_specimen,
    EXTRACT_DATE(
        COALESCE(txn.test_cancellation_date, txn.test_report_date),
        '%m/%d/%Y',
        cast({min_date} as date),
        cast(txn.vendor_file_date as date)
    )                                                                                   AS date_report,
    txn.test_orderid_hashed                                                             AS test_id,
    txn.panel_code                                                                      AS test_battery_local_id,
    txn.panel_name                                                                      AS test_battery_name,
    txn.test_code                                                                       AS test_ordered_local_id,
    CASE WHEN txn.test_name IS NOT NULL AND txn.methodology IS NOT NULL
               THEN CONCAT(txn.test_name, ' ', txn.methodology)
          WHEN txn.test_name IS NOT NULL THEN txn.test_name
          ELSE txn.methodology
      END                                                                               AS test_ordered_name,
    res.result_value                                                                    AS result,
    res.result_code                                                                     AS result_name, -- 20
    res.result_units_of_measure                                                         AS result_unit_of_measure,
    res.result_name                                                                     AS result_desc,
    CASE WHEN res.result_comments IS NOT NULL AND txn.level_of_service IS NOT NULL
               THEN CONCAT('Comments: ', res.result_comments, ' | Level of Service: ', txn.level_of_service)
         WHEN res.result_comments IS NOT NULL
              THEN CONCAT('Comments: ', res.result_comments)
         WHEN txn.level_of_service IS NOT NULL
              THEN CONCAT('Level of Service: ', txn.level_of_service)
          ELSE NULL
      END                                                                               AS result_comments,
    res.result_reference_range                                                          AS ref_range_alpha,
    txn.billing_icd_codes                                                               AS diagnosis_code,
    CASE WHEN txn.diagnosis_priority IS NOT NULL THEN 1 + txn.diagnosis_priority
          ELSE NULL
      END                                                                               AS diagnosis_code_priority,
    txn.cpt_code                                                                        AS procedure_code,
    CASE WHEN 0 <> LENGTH(CLEAN_UP_PROCEDURE_CODE(txn.cpt_code)) THEN 'HC'
          ELSE NULL
      END                                                                               AS procedure_code_qual,
    SUBSTR(UPPER(CLEAN_UP_ALPHANUMERIC_CODE(txn.cpt_modifier_id)), 1, 2)                AS procedure_modifier_1,
    txn.ordering_provider_npi_number                                                    AS ordering_npi, -- 30
    txn.insurance_company_id                                                            AS payer_id,
    CASE WHEN txn.insurance_company_id IS NOT NULL THEN 'INSURANCE_COMPANY_ID'
          ELSE NULL
      END                                                                               AS payer_id_qual,
    txn.insurance_company_name                                                          AS payer_name,
    txn.performing_organization_name                                                    AS lab_other_id,
    CASE WHEN txn.performing_organization_name IS NOT NULL THEN 'PERFORMING_ORGANIZATION_NAME'
          ELSE NULL
      END                                                                               AS lab_other_qual,
    txn.ordering_practice_name                                                          AS ordering_other_id,
    CASE WHEN txn.ordering_practice_name IS NOT NULL THEN 'ORDERING_PRACTICE_NAME'
          ELSE NULL
      END                                                                               AS ordering_other_qual,
    CASE WHEN txn.ordering_provider_last_name IS NOT NULL
           AND txn.ordering_provider_first_name IS NOT NULL
               THEN CONCAT(txn.ordering_provider_last_name, ', ', txn.ordering_provider_first_name)
          ELSE txn.ordering_provider_last_name
      END                                                                               AS ordering_name,
    txn.ordering_practice_address_line_1                                                AS ordering_address_1,
    txn.ordering_practice_city                                                          AS ordering_city, -- 40
    txn.ordering_practice_state                                                         AS ordering_state,
    txn.ordering_practice_zip_code                                                      AS ordering_zip,
    CASE WHEN UPPER(txn.test_status) IN ('CANCELLED', 'CANCELED') THEN 'CANCELED'
          ELSE NULL
      END                                                                               AS logical_delete_reason
 FROM neogenomics_meta_dedup txn
 LEFT OUTER JOIN neogenomics_results_dedup res
   ON txn.test_orderid_hashed = res.test_orderid_hashed
 LEFT OUTER JOIN neogenomics_payload_lag pay
   ON TRIM(UPPER(txn.patientid_hashed)) = TRIM(UPPER(pay.personid))
  AND txn.vendor_file_date > pay.prev_vendor_file_date
  AND txn.vendor_file_date <= pay.vendor_file_date
