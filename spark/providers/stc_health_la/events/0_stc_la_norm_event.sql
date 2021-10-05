select
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
    '251'                                                                                   AS data_feed,
    '1755'                                                                                  AS data_vendor,
    ----Patient Information
    CAP_AGE
        (
            VALIDATE_AGE
                (
                    pay.age,
                    CAST(EXTRACT_DATE(txn.date_administered, '%Y%m%d') AS DATE),
                    txn.date_of_birth
                )
        )                                                                                   AS patient_age,
    CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
         (
            pay.age,
            CAST(EXTRACT_DATE(txn.date_administered, '%Y%m%d') AS DATE),
            COALESCE(txn.date_of_birth, pay.yearofbirth)
         )                                                                                   AS patient_year_of_birth,
    MASK_ZIP_CODE
        (
            SUBSTR(COALESCE(txn.zip_code, pay.threedigitzip), 1, 3)
        )                                                                                   AS patient_zip3,
    CASE
        WHEN geo.geo_state_pstl_cd is null THEN NULL
        ELSE VALIDATE_STATE_CODE(geo.geo_state_pstl_cd)
    END                                                                                     AS patient_state,
    CASE
         WHEN txn.gender IS NULL AND pay.gender IS NULL THEN NULL
         WHEN SUBSTR(UPPER(txn.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gender), 1, 1)
         WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender), 1, 1)
        ELSE 'U'
    END                                                                                     AS patient_gender,

    -- Source record information
    txn.row_level_identifier                                                                AS source_record_id,
    CASE
        WHEN txn.row_level_identifier IS NOT NULL THEN 'RECORD_ID'
    END                                                                                     AS source_record_qual,

    -- event information
    CONCAT (
                'VACCINATION | MANUFACTURER: ', COALESCE(upper(txn.manufacturer), '')
            )                                                                               AS event,
    txn.dose_number                                                                         AS event_val,
    'DOSE_NUMBER'                                                                           AS event_val_uom,
    txn.dose_amount                                                                         AS event_units,
    CAP_DATE
    (
        CAST(EXTRACT_DATE(txn.date_administered, '%m/%d/%Y') AS DATE),
        CAST('{EARLIEST_SERVICE_DATE}'                       AS DATE),
        CAST('{VDR_FILE_DT}'                                 AS DATE)
    )                                                                                       AS event_date,
    CASE
        WHEN txn.date_administered IS NOT NULL THEN 'DATE_ADMINISTERED'
    END                                                                                     AS event_date_qual,
    txn.vaccine                                                                             AS event_category_code,
    CASE
        WHEN txn.vaccine IS NOT NULL THEN 'CVX_CODE'
    END                                                                                     AS event_category_code_qual,
    CONCAT (
            'LOT_NUMBER: ', COALESCE(txn.lot_number, ''),
            ' | EXPIRATION_DATE: ', COALESCE(CAST(EXTRACT_DATE(txn.date_administered, '%m/%d/%Y' ) AS DATE), ''),
            ' | ANATOMICAL_SITE: ', COALESCE(txn.anatomical_site, ''),
            ' | ANATOMICAL_ROUTE: ', COALESCE(txn.anatomical_route, '')
            )                                                                               AS event_category_name,
    CASE
        WHEN txn.series_complete_after_this_dose IS NULL THEN NULL
        WHEN SUBSTR(UPPER(txn.series_complete_after_this_dose), 1, 1) IN ('Y', 'N', 'U') THEN SUBSTR(UPPER(txn.series_complete_after_this_dose), 1, 1)
        ELSE NULL
    END                                                                                     AS event_category_flag,
    'SERIES_COMPLETE_AFTER_THIS_DOSE'                                                       AS event_category_flag_qual,


    'stc_health_la'                                                                            AS part_provider,
    CASE
          WHEN CAP_DATE
             (
                CAST(EXTRACT_DATE(txn.date_administered, '%m/%d/%Y')                                AS DATE),
                CAST(COALESCE('{EARLIEST_SERVICE_DATE}', '{AVAILABLE_START_DATE}')                  AS DATE),
                CAST('{VDR_FILE_DT}'                                                                AS DATE)
             ) IS NULL
             THEN '0_PREDATES_HVM_HISTORY'
         ELSE CONCAT
                 (
                   SUBSTR(CAST(EXTRACT_DATE(txn.date_administered, '%m/%d/%Y' ) AS DATE), 1, 4), '-',
                   SUBSTR(CAST(EXTRACT_DATE(txn.date_administered, '%m/%d/%Y' ) AS DATE), 6, 2), '-01'
                )
     END                                                                                 AS part_best_date
FROM  txn
LEFT OUTER JOIN matching_payload pay
 ON txn.hvjoinkey = pay.hvjoinkey
LEFT OUTER JOIN ref_geo_state geo
on COALESCE(txn.state, pay.state, 'x') = geo.geo_state_nm
