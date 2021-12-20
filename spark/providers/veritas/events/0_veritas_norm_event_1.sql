SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    payl.hvid                                                                               AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    --SPLIT(payl.input_file_name, '/')[SIZE(SPLIT(payl.input_file_name, '/')) - 1]            AS data_set,
    'veritas'                                                                               AS data_set,
    '263'                                                                                   AS data_feed,
    '1861'                                                                                  AS data_vendor,
    CAP_AGE
    (
        VALIDATE_AGE(
             SUBSTR(CURRENT_DATE(), 1,4) - CAST(payl.yearofbirth AS INT),
           CAST(
                SUBSTR(EXTRACT_DATE(payl.deathMonth, 1, 10), '%Y-%m-%d' )
                AS DATE),
            SUBSTR(payl.yearofbirth, 1, 4)
        )
    )                                                                                       AS patient_age,
    payl.yearofbirth                                                                        AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(payl.threedigitzip, 1, 3))                                         AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(payl.state))                                                  AS patient_state,
    CASE
        WHEN payl.gender IS NULL THEN NULL
        WHEN SUBSTR(UPPER(payl.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(payl.gender), 1, 1)
        ELSE 'U'
    END                                                                                     AS patient_gender,
    payl.claimid                                                                            AS source_record_id,
    CASE
        WHEN payl.claimid IS NOT NULL THEN 'SOURCE_RECORD_ID'
    ELSE NULL
    END                                                                                     AS source_record_qual,
    CAST('{VDR_FILE_DT}' AS DATE)                                                            AS source_record_date,
    'DEATH'                                                                                 AS event,
    CONCAT
        (
            SUBSTR(payl.deathmonth, 3,4), '-',
            SUBSTR(payl.deathmonth, 1,2), '-',
            '01'
        )                                                                                   AS event_date,
--    'DEATH'                                                                                 AS event_date_qual,
    'veritas'                                                                               AS part_provider,
    CONCAT
        (
            SUBSTR(payl.deathmonth, 3,4), '-',
            SUBSTR(payl.deathmonth, 1,2)
        )                                                                                   AS part_best_date

FROM    matching_payload payl
WHERE   payl.claimid NOT IN ('traceabilitynbr')
AND     VALIDATE_STATE_CODE(UPPER(payl.state)) IS NOT NULL
AND     lower(cast(payl.multiMatch as string)) IN ('true')
-- AND     multiMatch = 'false'
and     (hvid is null or not exists (select d.hvid from matching_payload d where d.hvid = payl.hvid group by d.hvid having count(1) > 1))
