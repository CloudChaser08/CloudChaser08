SELECT
    monotonically_increasing_id()                                                       AS record_id,
    current_date()                                                                      AS created,
    split(txn.input_file_name, '/')[size(split(txn.input_file_name, '/'))-1]            AS data_set,
    txn.accessionid                                                                     AS claim_id,
    COALESCE(pay.hvid, CONCAT('85_', COALESCE(txn.patientobjectid, '')))                AS hvid,
    '07'                                                                                AS model_version,
    '85'                                                                                AS data_feed,
    '335'                                                                               AS data_vendor,
    CLEAN_UP_GENDER(COALESCE(txn.patientgender, pay.gender, 'U'))                       AS patient_gender,
    CAP_AGE
        (
            VALIDATE_AGE
                (
                    pay.age, 
                    CAST(EXTRACT_DATE(txn.dateofservice, '%Y/%m/%d') AS DATE),
                    COALESCE(YEAR(txn.patientdateofbirth), pay.yearofbirth)
                )
        )                                                                               AS patient_age,
    CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.dateofservice, '%Y/%m/%d') AS DATE),
            COALESCE(YEAR(txn.patientdateofbirth), pay.yearofbirth)
        )                                                                               AS patient_year_of_birth,
    MASK_ZIP_CODE(COALESCE(txn.patientzipcode, pay.threedigitzip))                      AS patient_zip3,
    VALIDATE_STATE_CODE(COALESCE(txn.patientstate, pay.state, ''))                      AS patient_state,
    CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(txn.accessiondatetime, txn.receiveddatetime), '%Y/%m/%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{file_date}' as DATE)
	)                                                                               AS date_service,
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.dateofservice, '%Y/%m/%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{file_date}' as DATE)
        )                                                                               AS date_specimen,
    CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.finalsignoutdatetime, '%Y/%m/%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{file_date}' as DATE)
        )                                                                               AS date_report,
    txn.performinglabobjectid                                                           AS lab_id,
    txn.testcode                                                                        AS test_ordered_std_id,
    txn.testname                                                                        AS test_ordered_name,
    txn.procedurename                                                                   AS result_name,
    txn.microscopicdescription                                                          AS result_desc,
    CONCAT(
            'FINAL DIAGNOSIS: ', COALESCE(txn.finaldiagnosis, ''),
            '|COMMENT: ', COALESCE(txn.comment, ''), 
            '|CLINICAL INFORMATION: ', COALESCE(txn.clinicalinformation, ''), 
            '| SPECIMEN SOURCE: ', COALESCE(txn.specimensource, ''), 
            '| SPECIMEN LOCATION: ', COALESCE(txn.specimenlocation, '')
          )                                                                             AS result_comments,
    CLEAN_UP_DIAGNOSIS_CODE
        (
            txn.icdcode,
            NULL,
            CAST(EXTRACT_DATE(txn.dateofservice, '%Y/%m/%d') AS DATE)
        )                                                                               AS diagnosis_code,
    CLEAN_UP_PROCEDURE_CODE
        (
            CASE WHEN 0 = LOCATE('X', UPPER(txn.testcptcode)) 
                      THEN UPPER(txn.testcptcode) 
                 ELSE SUBSTR(UPPER(txn.testcptcode), 1, -1 + LOCATE('X', UPPER(txn.testcptcode))) 
             END
        )                                                                               AS procedure_code,
    CLEAN_UP_NPI_CODE
        (
            CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.orderingprovidernpi, '')))
                  AND '0000000000' <> TRIM(COALESCE(txn.orderingprovidernpi, ''))
                      THEN txn.orderingprovidernpi
                 ELSE NULL
             END
        )                                                                               AS ordering_npi,
    txn.performinglabname                                                               AS lab_other_id,
    (CASE WHEN txn.performinglabname IS NOT NULL THEN 'PERFORMINGLABNAME'
          ELSE NULL
      END)                                                                              AS lab_other_qual,
    txn.orderingpracticeid                                                              AS ordering_other_id,
    (CASE WHEN txn.orderingpracticeid IS NOT NULL THEN 'ORDERINGPRACTICEID'
          ELSE NULL
      END)                                                                              AS ordering_other_qual,
    (CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.orderingproviderlastname, ''))) 
           AND 0 <> LENGTH(TRIM(COALESCE(txn.orderingproviderfirstname, '')))
               THEN CONCAT
                    (
                        txn.orderingproviderlastname, ', ', 
                        txn.orderingproviderfirstname, 
                        CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.orderingprovidermiddleinitial, ''))) 
                                  THEN CONCAT(' ', txn.orderingprovidermiddleinitial) 
                             ELSE '' 
                         END
                    ) 
          ELSE txn.orderingproviderlastname 
      END)                                                                              AS ordering_name,
    txn.orderingproviderspecialty                                                       AS ordering_specialty,
    txn.orderingproviderid                                                              AS ordering_vendor_id,
    txn.orderingpracticeaddressline1                                                    AS ordering_address_1,
    txn.orderingpracticeaddressline2                                                    AS ordering_address_2,
    txn.orderingpracticecity                                                            AS ordering_city,
    VALIDATE_STATE_CODE(txn.orderingpracticestate)                                      AS ordering_state,
    txn.orderingpracticezipcode                                                         AS ordering_zip,
    'aurora_diagnostics'                                                                AS part_provider,
    (CASE WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE(
                                                    CAST(EXTRACT_DATE(COALESCE(txn.accessiondatetime, txn.receiveddatetime), '%Y/%m/%d') AS DATE), 
                                                    COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt, CAST('1901-01-01' AS DATE)),
                                                    CAST('{file_date}' as DATE)
                                                )
                                                , '')))
               THEN '0_PREDATES_HVM_HISTORY'
          ELSE CONCAT(
                        SUBSTR(COALESCE(txn.accessiondatetime, txn.receiveddatetime), 1, 4), '-',
                        SUBSTR(COALESCE(txn.accessiondatetime, txn.receiveddatetime), 6, 2)
                     )
      END)                                                                              AS part_best_date
 FROM auroradx_transactions_dedup txn
 LEFT OUTER JOIN auroradx_payload pay
   ON txn.hvjoinkey = pay.hvjoinkey
 LEFT OUTER JOIN ref_gen_ref esdt
   ON 1 = 1
  AND esdt.hvm_vdr_feed_id = 85
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 85
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
