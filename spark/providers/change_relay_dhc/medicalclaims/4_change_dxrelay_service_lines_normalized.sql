SELECT DISTINCT
    sln.claim_number                                                                        AS claimid,
    sln.record_type                                                                         AS recordtype,
    sln.service_line_number                                                                 AS linenumber,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_from_date         , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.received_date             , '%Y%m%d' ) AS DATE )
    )                                                                     AS servicefromdate,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_to_date           , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.received_date             , '%Y%m%d' ) AS DATE)
    )                                                                     AS servicetodate,


    CASE
        WHEN  sln.place_of_service IS NULL THEN NULL
        WHEN  LPAD(sln.place_of_service , 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN '99'
        WHEN  sln.place_of_service IS NOT NULL THEN LPAD(sln.place_of_service , 2, '0')
    END                                                                                     AS placeofserviceid,
    CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.procedure_code))                                   AS procedurecode,
    sln.procedure_code_qual                                                                 AS procedurecodequal,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_1), 1, 2))               AS proceduremodifier1,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_2), 1, 2))               AS proceduremodifier2,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_3), 1, 2))               AS proceduremodifier3,
    CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_4), 1, 2))               AS proceduremodifier4,
--    sln.service_line_charge_amount                                                          AS linecharges,             -- Investigated for any decimals in source. None found.
    CAST(
        CASE
            WHEN LENGTH(sln.service_line_charge_amount) <= 2 THEN  CONCAT('0.',sln.service_line_charge_amount)
        ELSE   CONCAT(SUBSTR(sln.service_line_charge_amount , 1, LENGTH(sln.service_line_charge_amount)-2),'.',SUBSTR(sln.service_line_charge_amount , LENGTH(sln.service_line_charge_amount)-1))
        END
        AS FLOAT)                                                                           AS linecharges,

    CAST(sln.unit_count AS FLOAT)                                                           AS unitcount,
    sln.revenue_code                                                                        AS revenuecode,
    sln.diagnosis_code_pointer_1                                                            AS diagnosiscodepointer1,
    sln.diagnosis_code_pointer_2                                                            AS diagnosiscodepointer2,
    sln.diagnosis_code_pointer_3                                                            AS diagnosiscodepointer3,
    sln.diagnosis_code_pointer_4                                                            AS diagnosiscodepointer4,
    CLEAN_UP_NUMERIC_CODE(sln.national_drug_code)                                           AS ndccode,                 -- Only remove alphanumeric characters per vendor, via Will 4/22/2021.
    CASE
        WHEN SUBSTR(UPPER(COALESCE(sln.emergency_indicator,'')), 1, 1) IN ('Y', 'N')  THEN SUBSTR(UPPER(sln.emergency_indicator), 1, 1)
        ELSE NULL
    END                                                                                     AS emergencyind,
     SPLIT(sln.input_file_name, '/')[SIZE(SPLIT(sln.input_file_name, '/')) - 1]              AS sourcefilename,
    'citra_riwaka'                                                                          AS data_vendor,
    CAST(NULL AS STRING)                                                                    AS dhcreceiveddate,         -- Set to NULL per Will 4/22/2021.
    CAST(NULL AS BIGINT)                                                                    AS rownumber,               -- Set to NULL per Will 4/22/2021.
    CAST(NULL AS INT)                                                                       AS fileyear,
    CAST(NULL AS INT)                                                                       AS filemonth,
    CAST(NULL AS INT)                                                                       AS fileday,
    '220'                                                                                   AS data_feed,
    CURRENT_DATE()                                                                          AS created,
    'change_relay'                                                                          AS part_provider,
    CASE
        WHEN
            COALESCE
            (
                CAST(EXTRACT_DATE(sln.service_from_date       , '%Y%m%d' ) AS DATE),
                CAST(EXTRACT_DATE(CONCAT('20',clm.received_date), '%Y%m%d' ) AS DATE)
            ) IS NULL THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                   SUBSTR(sln.service_from_date, 1, 4), '-',
                   SUBSTR(sln.service_from_date, 5, 2), '-01'
                )
    END                                                                    AS part_best_date
FROM change_dxrelay_service_lines sln
LEFT OUTER JOIN change_dxrelay_claims clm ON clm.claim_tcn_id = sln.claim_number
WHERE sln.claim_number IS NOT NULL
ORDER BY 1