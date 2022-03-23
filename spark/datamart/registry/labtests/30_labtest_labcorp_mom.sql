SELECT
    a.*
FROM
(
  SELECT
      /* claim_id */
      rslt.specimen_number                                                                     AS claim_id,
      CASE
        WHEN payl.hvid IS NOT NULL THEN payl.hvid
        WHEN rslt.patient_id IS NOT NULL THEN CONCAT('269_',rslt.patient_id)
        ELSE NULL
      END                                                                                     AS hvid,
      CURRENT_DATE()                                                                          AS created,
      '01'                                                                                    AS model_version,
      rslt.data_set                                                                           AS data_set,
      '269'                                                                                   AS data_feed,
      '10'                                                                                    AS data_vendor,
      CAST(NULL AS DATE)                                                                      AS date_service,
     /* date_specimen */
    CASE
      WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
        OR CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
      ELSE CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)
      END                                                                                     AS date_specimen,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- date_report
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(rslt.result_date, '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(rslt.result_date, '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(rslt.result_date, '%Y-%m-%d') AS DATE)
    END                                                                                       AS  date_report,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- loinc_code
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN LENGTH(TRIM(CLEAN_UP_LOINC_CODE(rslt.loinc_code))) = 0 OR  LENGTH(TRIM(COALESCE(rslt.loinc_code ,'' ))) = 0  THEN NULL
      ELSE CLEAN_UP_LOINC_CODE(loinc_code)
    END                                                                                        AS loinc_code,
      rslt.test_number                                                                         AS test_number,
      rslt.test_ordered_code                                                                   AS test_ordered_std_id,
      rslt.test_ordered_name                                                                   AS test_ordered_name,
      /* result_id */
      rslt.test_number                                                                         AS result_id,
      /* result */
      CASE
          WHEN rslt.result_dec = 0.0000 AND  xwalk.abbrev IS NOT NULL
             THEN COALESCE(xwalk.text, xwalk.abbrev)
          when rslt.result_dec = 0.0000 and rslt.result_abbrv  is not NULL
              then rslt.result_abbrv
          ELSE rslt.result_dec
      END                                                                                      AS result,
      /*   result_name    */
      CASE
          WHEN rslt.test_name = '.' OR rslt.test_name LIKE '*%'
          THEN null
        ELSE rslt.test_name
      END                                                                                      AS result_name,
      /*   ref_range       */
      CONCAT
        (
          CASE
            WHEN rslt.normal_dec_low = 0.0 AND rslt.normal_dec_high = 0.0  THEN NULL
            ELSE rslt.normal_dec_low
          END
          ,'-',
          CASE
            WHEN rslt.normal_dec_low = 0.0 AND rslt.normal_dec_high = 0.0  THEN NULL
            ELSE rslt.normal_dec_high
          END
       )                                                                                       AS ref_range,
      /* abnormal_flag */
      CASE
        WHEN rslt.result_abn_code IN ('H','L','A', '+','-','>','<')  THEN 'Y'
        WHEN rslt.result_abn_code = 'N'                              THEN 'N'
       ELSE NULL
      END                                                                                      AS abnormal_flag,
      /* diagnosis_code */
      CLEAN_UP_ALPHANUMERIC_CODE
      (
        CASE
          WHEN pvt.diagnosis_code_qual = '01' THEN
                  CASE
                      WHEN pvt.diagnosis_code RLIKE ('^(E95.*|E96.*|E97.*|E99.*|E928.*|E8[0-4].*)$') THEN NULL
                      WHEN pvt.diagnosis_code RLIKE ('^V854[1-5]$')                                  THEN 'V854'
                      ELSE pvt.diagnosis_code
                  END
          WHEN pvt.diagnosis_code_qual = '02' THEN
                  CASE
                      WHEN pvt.diagnosis_code RLIKE ('^(Y35.*|Y36.*|Y37.*|Y38.*|X92.*|Y09.*|X52.*|V.*)$') THEN NULL
                      WHEN pvt.diagnosis_code RLIKE ('^Z684[1-5]$')                                       THEN 'Z684'
                      ELSE pvt.diagnosis_code
                  END
          WHEN pvt.diagnosis_code_qual IS NULL AND CAST(EXTRACT_DATE(COALESCE(rslt.result_date, rslt.pat_dos), '%Y-%m-%d') AS DATE) <  CAST('2015-10-01' AS DATE) THEN
                  CASE
                      WHEN pvt.diagnosis_code RLIKE ('^(E95.*|E96.*|E97.*|E99.*|E928.*|E8[0-4].*)$') THEN NULL
                      WHEN pvt.diagnosis_code RLIKE ('^V854[1-5]$')                                  THEN 'V854'
                      ELSE pvt.diagnosis_code
                  END
          WHEN pvt.diagnosis_code_qual IS NULL AND CAST(EXTRACT_DATE(COALESCE(rslt.result_date, rslt.pat_dos), '%Y-%m-%d') AS DATE) >=  CAST('2015-10-01' AS DATE) THEN
                  CASE
                      WHEN pvt.diagnosis_code RLIKE ('^(Y35.*|Y36.*|Y37.*|Y38.*|X92.*|Y09.*|X52.*|V.*)$') THEN NULL
                      WHEN pvt.diagnosis_code RLIKE ('^Z684[1-5]$')                                       THEN 'Z684'
                      ELSE pvt.diagnosis_code
                  END
          ELSE pvt.diagnosis_code
        END
      )                                                                                       AS diagnosis_code,
      pvt.diagnosis_code_qual                                                                 AS diagnosis_code_qual,
      pvt.diagnosis_code_priority                                                             AS diagnosis_code_priority,

      rslt.perf_lab_code                                                                       AS lab_other_id,
      /* lab_other_qual */
      CASE
          WHEN rslt.perf_lab_code IS NOT NULL THEN 'PERFORMING_LAB'
      END                                                                                      AS lab_other_qual,
      /*  NPI   */
      MD5(rslt.npi)                                                                            AS ordering_other_id,  ---HASHed value
      CASE
          WHEN rslt.npi IS NOT NULL THEN 'ORDERING_NPI'
      END                                                                                      AS ordering_other_qual,
      rslt.specialty_code                                                                      AS ordering_specialty,

      'labcorp'                                                                                AS part_provider,
     /* part_best_date   */
      CASE
          WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  < CAST('{AVAILABLE_START_DATE}'  AS DATE)
            OR CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                       AS DATE)
         THEN '0_PREDATES_HVM_HISTORY'
      ELSE CONCAT(SUBSTR(SUBSTR(rslt.pat_dos, 1, 10), 1, 8), '01')
      END                                                                                    AS part_best_date

  FROM labtest_labcorp_txn_result rslt
  LEFT OUTER JOIN matching_payload payl ON rslt.hvJoinKey = payl.hvJoinKey
  LEFT OUTER JOIN labtest_labcorp_dx_code_pivot pvt ON pvt.specimen_number = rslt.specimen_number
                                  and pvt.test_name = rslt.test_name
                                  and pvt.test_ordered_code = rslt.test_ordered_code
  LEFT OUTER JOIN labcorp_abbr_xwalk xwalk ON rslt.result_abbrv = xwalk.abbrev
                                            and NOT (
                                            xwalk.abbrev like '@%' OR
                                            lower(xwalk.text) like '%review%by%' OR
                                            lower(xwalk.text) like '%gist%' OR
                                            lower(xwalk.text) like '%cytology%' or
                                            lower(xwalk.text) like '%pathology%' or
                                            lower(xwalk.text) like '%cytotech%' or
                                            lower(xwalk.text) like '%phd%' or
                                            lower(xwalk.text) like '% dr %')

  ------------------------- Additional code to include only hvid from MOM-baby link table
  INNER JOIN
  (
    SELECT adult_hvid AS hvid FROM _mom_masterset WHERE adult_hvid IS NOT NULL
    UNION ALL
    SELECT child_hvid AS hvid FROM _mom_masterset WHERE child_hvid IS NOT NULL
  ) mom ON payl.hvid = mom.hvid
  GROUP BY 1,  2,  3,  4,  5,  6,  7,  8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
           21, 22, 23, 24, 25, 26, 27, 28, 29
) a
