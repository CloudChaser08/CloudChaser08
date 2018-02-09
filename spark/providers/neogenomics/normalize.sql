SELECT
    t.test_order_id                         AS claim_id,
    mp.hvid                                 AS hvid,
    mp.gender                               AS patient_gender,
    mp.age                                  AS patient_age,
    mp.yearOfBirth                          AS patient_year_of_birth,
    mp.threeDigitZip                        AS patient_zip3,
    mp.state                                AS patient_state,
    extract_date(
        t.test_ordered_date, '%m/%d/%Y'
        )                                   AS date_service,
    extract_date(
        t.specimen_collected_date, '%m/%d/%Y'
        )                                   AS date_specimen,
    COALESCE(
        extract_date(
            t.test_canceled_date, '%m/%d/%Y'
            ),
        extract_date(
            t.test_reported_date, '%m/%d/%Y'
            )
        )                                   AS date_report,
    t.test_order_id                         AS test_id,
    t.panel_code                            AS test_battery_local_id,
    t.panel_name                            AS test_battery_name,
    t.test_code                             AS test_ordered_local_id,
    CONCAT(t.test_name, ' ', t.technology)  AS test_ordered_name,
    t.level_of_service                      AS result_comments,
    SPLIT(
        clean_neogenomics_diag_list(t.icd_code), ','
        )[n.n]                              AS diagnosis_code,
    n.n + 1                                 AS diagnosis_code_priority,
    t.client_zip                            AS ordering_zip,
    CASE
      WHEN UPPER(t.status) = 'CANCELLED'
      THEN 'CANCELED'
    END                                     AS logical_delete_reason
    {result_columns}
FROM transactional_tests t
    {result_join}
    LEFT JOIN matching_payloads mp ON UPPER(t.patient_id) = UPPER(mp.personid)
        AND t.vendor_date > mp.prev_vendor_date
        AND t.vendor_date <= mp.vendor_date
    CROSS JOIN diagnosis_exploder n

WHERE (
        SPLIT(clean_neogenomics_diag_list(t.icd_code), ',')[n.n] IS NOT NULL
        AND SPLIT(clean_neogenomics_diag_list(t.icd_code), ',')[n.n] != ''
        )
    OR ((
            t.icd_code IS NULL
            OR REGEXP_REPLACE(t.icd_code, '[ ,]*', '') = ''
            )
        AND n.n = 0
        )
