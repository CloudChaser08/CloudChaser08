SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    SPLIT(pay.input_file_name, '/')[SIZE(SPLIT(pay.input_file_name, '/')) - 1]                AS data_set,
    '231'                                                                                   AS data_feed,
    '1765'                                                                                  AS data_vendor,
    CASE
        WHEN SUBSTR(CURRENT_DATE, 1, 4) - pay.yearofbirth > 84 THEN 1927
        WHEN SUBSTR(CURRENT_DATE, 1, 4) - pay.yearofbirth <= 84 THEN pay.yearofbirth
    END                                                                                     AS patient_year_of_birth,	 
    'DECEASED'                                                                              AS event,
    CONCAT
        (
            SUBSTR(pay.deathmonth, 3,4), '-',
            SUBSTR(pay.deathmonth, 1,2), '-',
            '01'
        )                                                                                   AS event_date,
    'DECEASED_DATE'                                                                         AS event_date_qual,
    'ladmf'                                                                                 AS part_provider,
    CONCAT
        (
            SUBSTR(pay.deathmonth, 3,4), '-',
            SUBSTR(pay.deathmonth, 1,2), '-01'
        )                                                                                   AS part_best_date       

FROM matching_payload pay