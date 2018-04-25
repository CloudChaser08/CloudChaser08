SELECT
    hvid                                    AS hvid,
    {date_input}                            AS source_version,
    yearOfBirth                             AS patient_year_of_birth,
    threeDigitZip                           AS patient_zip3,
    UPPER(state)                            AS patient_state,
    gender                                  AS patient_gender,
    claimId                                 AS source_record_id,
    'ACXIOMID'                              AS source_record_qual,
    extract_date(
        {date_input},
        '%Y-%m-%d'
    )                                       AS source_record_date
FROM matching_payload
