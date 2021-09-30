select
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    CASE 
        WHEN pay.matchStatus = 'multi_match' then null 
        ELSE pay.hvid END                                                                   AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
    '27'                                                                                    AS data_feed,
    '49'                                                                                    AS data_vendor,
    CAST(NULL AS STRING) AS source_version,
    ----Patient Information
    CAP_AGE (pay.age)                                                                       AS patient_age,
    CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
	    (
            pay.age,
            CAST(CONCAT(
                SUBSTRING(pay.deathMonth, 3, 6),
                '-',
                SUBSTRING(pay.deathMonth, 0, 2),
                '-01'
                ) AS DATE),
            pay.yearofbirth
	    )                                                                                   AS patient_year_of_birth,

    CAST(NULL AS STRING) AS patient_zip,
    MASK_ZIP_CODE
        (
            SUBSTR(pay.threedigitzip, 1, 3)
        )                                                                                   AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(pay.state))                                                   AS patient_state,
    CAST(NULL AS STRING) AS patient_gender,

    -- Source record information
    txn.id                                                                                  AS source_record_id,
    CAST('OBITUARY' AS STRING)                                                              AS source_record_qual,
    CAST(NULL AS DATE) AS source_record_date,
    CAST(NULL AS STRING) AS patient_group,
    -- event information
    CAST('DEATH' AS STRING)                                                                 AS event,
    CAST(NULL AS STRING) AS event_val,
    CAST(NULL AS STRING) AS event_val_uom,
    CAST(NULL AS STRING) AS event_units,
    CAST(NULL AS STRING) AS event_units_uom,
    CAST(CONCAT(
            SUBSTRING(pay.deathMonth, 3, 6),
            '-',
            SUBSTRING(pay.deathMonth, 0, 2),
            '-01'
            ) AS DATE)                                                                      AS event_date,
    CAST(NULL AS STRING) AS event_date_qual,
    CAST(NULL AS STRING) AS event_zip,
    CAST(NULL AS STRING) AS event_revenue,
    CAST(NULL AS STRING) AS event_category_code,
    CAST(NULL AS STRING) AS event_category_code_qual,
    CAST(NULL AS STRING) AS event_category_name,
    CAST(NULL AS STRING) AS event_category_flag,
    CAST(NULL AS STRING) AS event_category_flag_qual,
    CAST(NULL AS STRING) AS logical_delete_reason,

    'obituarydata'                                                                          AS part_provider,
    CONCAT(
            SUBSTRING(pay.deathMonth, 3, 6),
            '-',
            SUBSTRING(pay.deathMonth, 0, 2),
            '-01'
            )                                                                               AS part_best_date
FROM  txn
LEFT OUTER JOIN matching_payload pay
 ON txn.hv_join_key = pay.hvJoinKey
    --AND pay.hvid IS NOT NULL
    --AND pay.matchStatus != 'multi_match'
