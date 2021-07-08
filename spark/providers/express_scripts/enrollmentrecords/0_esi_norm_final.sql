SELECT
    MONOTONICALLY_INCREASING_ID()                               AS record_id,
    phi.hvid                                                    AS hvid,
    CURRENT_DATE()                                              AS created,
    '1'                                                         AS model_version,
    CONCAT(
        '10130X001_HV_RX_ENROLLMENT_D',
        SUBSTR('{VDR_FILE_DT}', 1, 4),
        SUBSTR('{VDR_FILE_DT}', 6, 2),
        SUBSTR('{VDR_FILE_DT}', 9, 2), '.txt'
    )                                                           AS data_set,
    '61'                                                        AS data_feed,
    '17'                                                        AS data_vendor,
    CAST(NULL AS STRING)                                        AS source_version,
    CAST(NULL AS STRING)                                        AS patient_age,
    phi.year_of_birth                                           AS patient_year_of_birth,
    phi.zip                                                     AS patient_zip3,
    zip3.state                                                  AS patient_state,
    phi.gender                                                  AS patient_gender,
    CAST(NULL AS STRING)                                        AS source_record_id,
    CAST(NULL AS STRING)                                        AS source_record_qual,
    CAST(EXTRACT_DATE(e.operation_date, '%Y%m%d') AS DATE)      AS source_record_date,
    CAST(EXTRACT_DATE(e.start_date, '%Y%m%d') AS DATE)          AS date_start,
    CAST(EXTRACT_DATE(e.end_date, '%Y%m%d') AS DATE)            AS date_end,
    CAST(NULL AS STRING)                                        AS benefit_type,
    'express_scripts'                                           AS part_provider,
    CONCAT(
        SUBSTR('{VDR_FILE_DT}', 1, 4), '-',
        SUBSTR('{VDR_FILE_DT}', 6, 2)
        )                                                       AS part_best_date
FROM transaction e
    LEFT JOIN matching_payload mp ON e.hvjoinkey = mp.hvJoinKey
    INNER JOIN local_phi phi ON mp.patientId = phi.patient_id
    LEFT JOIN zip3_to_state zip3 ON zip3.zip3 = phi.zip
