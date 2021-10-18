
select
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    SPLIT(pay.input_file_name, '/')[SIZE(SPLIT(pay.input_file_name, '/')) - 1]              AS data_set,
    '50'                                                                                    AS data_feed,
    '229'                                                                                   AS data_vendor,
    '{VDR_FILE_DT}'                                                                         AS source_version,
    ----Patient Information
    CAST(NULL AS STRING)                                                                    AS patient_age,
    CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
	    (
            NULL,
            CAST(NULL AS DATE),
            pay.yearOfBirth
	    )                                                                                   AS patient_year_of_birth,

    CAST(NULL AS STRING) AS patient_zip,
    MASK_ZIP_CODE
        (
            SUBSTR(pay.threedigitzip, 1, 3)
        )                                                                                   AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(pay.state))                                                   AS patient_state,
    /* patient_gender */
    CASE
        WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE NULL
    END                                                                                     AS patient_gender,

    -- Source record information
    pay.claimid                                                                             AS source_record_id,
    'ACXIOMID'                                                                              AS source_record_qual,
    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)                                 AS source_record_date,
    CAST(NULL AS STRING) AS patient_group,
    -- event information
    CAST(NULL AS STRING) AS event,
    CAST(NULL AS STRING) AS event_val,
    CAST(NULL AS STRING) AS event_val_uom,
    CAST(NULL AS STRING) AS event_units,
    CAST(NULL AS STRING) AS event_units_uom,
    CAST(NULL AS DATE)                                                                      AS event_date,
    CAST(NULL AS STRING) AS event_date_qual,
    CAST(NULL AS STRING) AS event_zip,
    CAST(NULL AS STRING) AS event_revenue,
    CAST(NULL AS STRING) AS event_category_code,
    CAST(NULL AS STRING) AS event_category_code_qual,
    CAST(NULL AS STRING) AS event_category_name,
    CAST(NULL AS STRING) AS event_category_flag,
    CAST(NULL AS STRING) AS event_category_flag_qual,
    CAST(CASE WHEN aid.aid IS NOT NULL THEN 'DELETE' else NULL END AS STRING)               AS logical_delete_reason,
    'acxiom'                                                                                AS part_provider,
    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)                                 AS part_best_date
FROM  matching_payload pay
    LEFT OUTER JOIN exclude_acxiom_ids aid
        ON pay.claimid = aid.aid
