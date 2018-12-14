SELECT
    UPPER(obfuscate_hvid(hvid, 'hvidhv000710'))                     AS hvid,
    hc1.claimId                                                     AS hc1_record_id,
    mph.claimId                                                     AS mph_record_id,
    COALESCE(hc1.gender, mph.gender, tests.patient_gender)          AS patient_gender,
    COALESCE(hc1.yearOfBirth, mph.yearOfBirth)                      AS patient_yob,
    COALESCE(hc1.threeDigitZip, mph.threeDigitZip)                  AS patient_zip3,
    tests.patient_age_group                                         AS patient_age_group,
    tests.patient_id                                                AS patient_id,
    tests.result_positivity                                         AS result_positivity,
    tests.result_mix                                                AS result_mix,
    tests.result_date_month                                         AS result_date_month,
    tests.lab_order_id                                              AS lab_order_id,
    tests.provider_id                                               AS provider_id,
    tests.location_id                                               AS location_id,
    tests.test_group                                                AS test_group,
    tests.drug_grouping                                             AS drug_grouping,
    tests.patient_county_code                                       AS patient_county_code,
    tests.loinc_component                                           AS loinc_component,
    tests.provider_specialty                                        AS provider_specialty
FROM tests
    INNER JOIN matching_payload_hc1 hc1
        USING (hvjoinkey)
    FULL OUTER JOIN matching_payload_mph mph
        USING (hvid)
