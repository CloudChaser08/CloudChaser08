SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    SPLIT(pay.input_file_name, '/')[SIZE(SPLIT(pay.input_file_name, '/')) - 1]              AS data_set,
    '214'                                                                                   AS data_feed,
    '665'                                                                                   AS data_vendor,
    pay.age                                                                                 AS patient_age,
    pay.yearofbirth                                                                         AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(pay.state))                                                   AS patient_state,
	/* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE NULL
    END                                                                                     AS patient_gender,
    pay.claimid                                                                             AS source_record_id,
    CASE
        WHEN pay.claimid IS NOT NULL THEN 'ADSTRA_CLAIMID'
    ELSE NULL
    END                                                                                     AS source_record_qual,
    extract_date(
        {date_input},
        '%Y-%m-%d'
    )                                                                                       AS source_record_date
FROM matching_payload pay
