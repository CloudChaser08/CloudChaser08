SELECT
    /* claim_id */
    rslt.specimen_number                                                                    AS claim_id,
    CASE
      WHEN payl.hvid IS NOT NULL THEN payl.hvid
      WHEN rslt.patient_id IS NOT NULL THEN CONCAT('269_',rslt.patient_id)
      ELSE NULL
    END                                                                                     AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    rslt.data_set                                                                           AS data_set,
	'269'                                                                                   AS data_feed,
	'10'                                                                                    AS data_vendor,
	/* patient_gender */
    CASE
        WHEN UPPER(rslt.patient_sex) IN ('F', 'M', 'U')               THEN UPPER(rslt.patient_sex)
        WHEN UPPER(payl.gender)      IN ('F', 'M','U')                THEN UPPER(payl.gender)
        WHEN payl.gender IS NOT NULL OR  rslt.patient_sex IS NOT NULL THEN 'U'
    END                                                                                    AS patient_gender,
	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
	(
        null,
	    CASE
          WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
            OR CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
          ELSE CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)
        END,
	    payl.yearofbirth
	)                                                                                       AS patient_year_of_birth,
	MASK_ZIP_CODE
    (
        COALESCE(SUBSTR(rslt.patient_zip5,1,3), payl.threeDigitZip)
    )                                                                                       AS patient_zip3,

    VALIDATE_STATE_CODE
    (
        UPPER(COALESCE(rslt.patient_state, payl.state, ''))
    )                                                                                       AS patient_state,
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
      WHEN LENGTH(TRIM(CLEAN_UP_LOINC_CODE(loinc_code))) = 0 OR  LENGTH(TRIM(COALESCE(loinc_code ,'' ))) = 0  THEN NULL
      ELSE CLEAN_UP_LOINC_CODE(loinc_code)
    END                                                                                       AS loinc_code,
    rslt.test_number                                                                          AS test_number,
    rslt.test_ordered_code                                                                    AS test_ordered_std_id,
    rslt.test_ordered_name                                                                    AS test_ordered_name,
    rslt.test_number                                                                          AS result_id,
    /* result */
     CASE

        WHEN rslt.result_dec = 0.0000 AND  xwalk.abbrev IS NOT NULL THEN COALESCE(xwalk.text, xwalk.abbrev)
        WHEN rslt.result_dec = 0.0000 AND rslt.result_abbrv IS NOT NULL THEN rslt.result_abbrv
        ELSE rslt.result_dec
    END                                                                                       AS result,
    /*   result_name    */
    CASE
        WHEN rslt.test_name = '.' OR rslt.test_name LIKE '*%' THEN NULL
    ELSE rslt.test_name
    END                                                                                       AS result_name,
    rslt.rslt_comments                                                                        AS result_comments,
    /*   ref_range_low       */
    CASE
        WHEN rslt.normal_dec_low = 0.0 and rslt.normal_dec_high = 0.0
        THEN null
       ELSE rslt.normal_dec_low
    END                                                                                       AS ref_range_low,
   /*   ref_range_high       */
    CASE
        WHEN rslt.normal_dec_low = 0.0 and rslt.normal_dec_high = 0.0
        THEN null
       ELSE rslt.normal_dec_high
    END                                                                                       AS ref_range_high,
    /* abnormal_flag */
    CASE
    WHEN rslt.result_abn_code IN ('H','L','A', '+','-','>','<')  THEN 'Y'
    WHEN rslt.result_abn_code = 'N'                              THEN 'N'
    END                                                                                      AS abnormal_flag,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- diagnosis_code
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN
      LENGTH(TRIM(
        CLEAN_UP_DIAGNOSIS_CODE
        (
            pvt.diagnosis_code,
            pvt.diagnosis_code_qual,
            CASE
            WHEN CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
              OR CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
            ELSE CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)
            END
         )
     )) = 0 THEN NULL
     ELSE
       CLEAN_UP_DIAGNOSIS_CODE
        (
            pvt.diagnosis_code,
            pvt.diagnosis_code_qual,
            CASE
            WHEN CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
              OR CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
            ELSE CAST(EXTRACT_DATE(COALESCE(rslt.result_date, SUBSTR(rslt.pat_dos, 1, 10)), '%Y-%m-%d') AS DATE)
            END
         )
       END                                                                                  AS diagnosis_code,
    pvt.diagnosis_code_qual                                                                 AS diagnosis_code_qual,
    pvt.diagnosis_code_priority                                                             AS diagnosis_code_priority,

    CLEAN_UP_NPI_CODE(rslt.npi)                                                              AS ordering_npi,
    rslt.perf_lab_code                                                                       AS lab_other_id,
    /* lab_other_qual */
    CASE
        WHEN rslt.perf_lab_code IS NOT NULL THEN 'PERFORMING_LAB'
    END                                                                                      AS lab_other_qual,
    rslt.specialty_code                                                                      AS ordering_specialty,
    rslt.report_zip                                                                          AS ordering_zip,
    'labcorp'                                                                                AS part_provider,
    /* part_best_date */
     CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  < CAST('{AVAILABLE_START_DATE}'  AS DATE)
          OR CAST(EXTRACT_DATE(SUBSTR(rslt.pat_dos, 1, 10), '%Y-%m-%d') AS DATE)  > CAST('{VDR_FILE_DT}'                       AS DATE)
       THEN '0_PREDATES_HVM_HISTORY'
    ELSE CONCAT(SUBSTR(rslt.pat_dos, 1, 8),  '01')
    END                                                                                     AS part_best_date

FROM labtest_labcorp_txn_result rslt
LEFT OUTER JOIN matching_payload payl     ON rslt.hvJoinKey = payl.hvJoinKey
LEFT OUTER JOIN labtest_labcorp_dx_code_pivot pvt  ON pvt.specimen_number = rslt.specimen_number
                                AND pvt.test_name = rslt.test_name
                                AND pvt.test_ordered_code = rslt.test_ordered_code
LEFT OUTER JOIN labcorp_abbr_xwalk xwalk ON rslt.result_abbrv = xwalk.abbrev