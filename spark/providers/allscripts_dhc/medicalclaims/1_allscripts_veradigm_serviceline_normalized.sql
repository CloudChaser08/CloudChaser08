SELECT DISTINCT
    sln.entity_id                                                         AS claimid,
    sln.record_type                                                       AS recordtype,
    sln.charge_line_number                                                AS linenumber,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_from_date       , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE )
    )                                                                     AS servicefromdate,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_to_date         , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE)
    )                                                                     AS servicetodate,
    CASE
        WHEN      sln.place_of_service IS NULL THEN NULL
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN '99'
        WHEN      sln.place_of_service IS NOT NULL THEN LPAD(sln.place_of_service , 2, '0')
        ELSE NULL
    END                                                                   AS placeofserviceid,
    CLEAN_UP_ALPHANUMERIC_CODE
    (
        UPPER
            (
                COALESCE
                (
                    sln.std_chg_line_hcpcs_procedure_code,
                    sln.dme_chg_line_hcpcs_procedure_code
                )
            )
    )                                                                     AS procedurecode,
    CASE
        WHEN   COALESCE
                (
                    sln.std_chg_line_hcpcs_procedure_code,
                    sln.dme_chg_line_hcpcs_procedure_code
                ) IS NOT NULL THEN 'HCPCS'
        ELSE NULL
    END                                                                   AS procedurecodequal,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.hcpcs_modifier_1)),1 ,2)  AS proceduremodifier1,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.hcpcs_modifier_2)),1 ,2)  AS proceduremodifier2,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.hcpcs_modifier_3)),1 ,2)  AS proceduremodifier3,
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.hcpcs_modifier_4)),1 ,2)  AS proceduremodifier4,
    sln.line_charges                                                      AS linecharges,
    sln.units_of_service                                                  AS unitcount,
    CAST(NULL AS STRING)                                                  AS revenuecode,
    sln.diagnosis_code_pointer_1	                                      AS diagnosiscodepointer1,
    sln.diagnosis_code_pointer_2	                                      AS diagnosiscodepointer2,
    sln.diagnosis_code_pointer_3	                                      AS diagnosiscodepointer3,
    sln.diagnosis_code_pointer_4	                                      AS diagnosiscodepointer4,
    clean_up_numeric_code(sln.ndc_code)                                   AS ndccode,
    CASE
        WHEN SUBSTR(UPPER(sln.emergency_indicator), 1, 1) IN ('Y', 'N')  THEN SUBSTR(UPPER(sln.emergency_indicator), 1, 1)
        ELSE NULL
    END                                                                   AS emergencyind,
    SPLIT(sln.input_file_name, '/')[SIZE(SPLIT(sln.input_file_name, '/')) - 1] AS sourcefilename,
    'apollo'                                                              AS data_vendor,
    CAST(NULL AS STRING)                                                  AS dhcreceiveddate,
    CAST(NULL AS INT)                                                     AS rownumber,
    CAST(NULL AS INT)                                                     AS fileyear,
    CAST(NULL AS INT)                                                     AS filemonth,
    CAST(NULL AS INT)                                                     AS fileday,
    '26'                                                                  AS data_feed,
    'allscripts'                                                          AS part_provider,
    CASE
        WHEN
            COALESCE
            (
                CAST(EXTRACT_DATE(sln.service_from_date       , '%Y%m%d' ) AS DATE),
                CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE)
            ) IS NULL THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                   SUBSTR(sln.service_from_date, 1, 4), '-',
                   SUBSTR(sln.service_from_date, 5, 2), '-01'
                )
    END                                                                    AS part_best_date


FROM serviceline sln
LEFT OUTER JOIN  claim clm  on clm.entity_id = sln.entity_id
WHERE
  (
      sln.entity_id IS NOT NULL
   AND LOWER(sln.entity_id) <> 'entity_id'
   )
ORDER BY 1