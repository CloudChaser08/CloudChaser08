SELECT
    hvid                                                                                    AS hvid,
    {date_input}                                                                            AS source_version,
    age                                                                                     AS patient_age,
    yearofbirth                                                                             AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(threedigitzip, 1, 3))                                              AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(state))                                                       AS patient_state,
	/* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(gender), 1, 1)
        ELSE NULL
    END                                                                                     AS patient_gender,
    claimid                                                                                 AS source_record_id,
    CASE
        WHEN claimid IS NOT NULL THEN 'ADSTRA_CLAIMID'
    ELSE NULL
    END                                                                                     AS source_record_qual,
    extract_date(
        {date_input},
        '%Y-%m-%d'
)                                                                                           AS source_record_date
FROM matching_payload pay