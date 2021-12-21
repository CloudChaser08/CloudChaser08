SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    payl.hvid                                                                               AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
--    SUBSTR(SPLIT(payl.input_file_name, '/')[SIZE(SPLIT(payl.input_file_name, '/')) - 1], 21)
--                                                                                            AS data_set,
    REPLACE(SPLIT(payl.input_file_name, '/')[SIZE(SPLIT(payl.input_file_name, '/')) - 1], 'veritas', '')
                                                                                            AS data_set,

    '263'                                                                                   AS data_feed,
    '1861'                                                                                  AS data_vendor,


CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
      (0,
    VALIDATE_AGE(0,
       CAST(
            SUBSTR(EXTRACT_DATE(payl.deathMonth, 1, 10), '%Y-%m-%d' )
            AS DATE),
        SUBSTR(payl.yearofbirth, 1, 4)
    ),
    CASE
        WHEN SUBSTR(CURRENT_DATE(), 1,4) - CAST(payl.yearofbirth AS INT) >= 85 THEN 1927
        ELSE payl.yearofbirth
    END
)                                                                                           AS patient_year_of_birth,

--    payl.yearofbirth                                                                        AS patient_year_of_birth,
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
AND     payl.matchStatus <> "multiMatch"
-- AND     payl.multiMatch IN ('true')
-- AND     multiMatch = 'false'
and     (hvid is null or not exists (select d.hvid from matching_payload d where d.hvid = payl.hvid group by d.hvid having count(1) > 1))
