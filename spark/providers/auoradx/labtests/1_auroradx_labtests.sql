SELECT
	txn.accession_id                                                                     AS claim_id,
	COALESCE(pay.hvid, CONCAT('85_', txn.patient_object_id))                              AS hvid,
	'07'                                                                                AS model_version,
	'85'                                                                                AS data_feed,
	'335'                                                                               AS data_vendor,
	CLEAN_UP_GENDER(COALESCE(txn.patient_gender, pay.gender, 'U'))                       AS patient_gender,
	CAP_AGE
	    (
    	    VALIDATE_AGE
        	    (
                    pay.age, 
                    CAST(EXTRACT_DATE(txn.date_of_service, '%Y/%m/%d') AS DATE),
                    COALESCE(YEAR(txn.patient_date_of_birth), pay.yearofbirth)
                )
        )                                                                               AS patient_age,
	CAP_YEAR_OF_BIRTH
	    (
            pay.age,
            CAST(EXTRACT_DATE(txn.date_of_service, '%Y/%m/%d') AS DATE),
            COALESCE(YEAR(txn.patient_date_of_birth), pay.yearofbirth)
        )												    AS patient_year_of_birth,
	MASK_ZIP_CODE(COALESCE(txn.patient_zip_code, pay.threedigitzip))				    AS patient_zip3,
	VALIDATE_STATE_CODE(COALESCE(txn.patient_state, pay.state, ''))					    AS patient_state,
        EXTRACT_DATE(txn.date_of_service, '%Y/%m/%d')							    AS date_service,
        EXTRACT_DATE(COALESCE(txn.accession_datetime, txn.received_datetime), '%Y/%m/%d')		    AS date_specimen,
        EXTRACT_DATE(txn.final_signout_datetime, '%Y/%m/%d')						    AS date_report,
	txn.performing_lab_object_id									    AS lab_id,
	txn.specimen_id											    AS test_id,
	txn.specimen_number										    AS test_number,
	txn.test_code											    AS test_ordered_local_id,
	txn.test_name											    AS test_ordered_name,
	txn.final_diagnosis										    AS result,
	txn.microscopic_description									    AS result_desc,
	(CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.comment, ''))) 
	       AND 0 <> LENGTH(TRIM(COALESCE(txn.clinical_information, ''))) 
	           THEN CONCAT(txn.comment, ' | ', txn.clinical_information) 
	      WHEN 0 <> LENGTH(TRIM(COALESCE(txn.comment, ''))) 
	           THEN txn.comment 
	      ELSE txn.clinical_information 
	  END)                                                                              AS result_comments,
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        txn.icd_code,
	        NULL,
	        CAST(EXTRACT_DATE(txn.date_of_service, '%Y/%m/%d') AS DATE)
	    )                                                                               AS diagnosis_code,
	CLEAN_UP_PROCEDURE_CODE
	    (
	        CASE WHEN 0 = LOCATE('X', UPPER(txn.test_cpt_code)) 
	                  THEN UPPER(txn.test_cpt_code) 
	             ELSE SUBSTR(UPPER(txn.test_cpt_code), 1, -1 + LOCATE('X', UPPER(txn.test_cpt_code))) 
	         END
	    )                                                                               AS procedure_code,
	CLEAN_UP_NPI_CODE
	    (
	        CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.ordering_provider_npi, '')))
	              AND '0000000000' <> TRIM(COALESCE(txn.ordering_provider_npi, ''))
	                  THEN txn.ordering_provider_npi
	             ELSE NULL
	         END
	    )                                                                               AS ordering_npi,
	txn.performing_lab_name                                                               AS lab_other_id,
	(CASE WHEN txn.performing_lab_name IS NOT NULL THEN 'PERFORMINGLABNAME'
	      ELSE NULL
	  END)                                                                              AS lab_other_qual,
	txn.ordering_practice_id                                                              AS ordering_other_id,
	(CASE WHEN txn.ordering_practice_id IS NOT NULL THEN 'ORDERINGPRACTICEID'
	      ELSE NULL
	  END)                                                                              AS ordering_other_qual,
	(CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.ordering_provider_last_name, ''))) 
	       AND 0 <> LENGTH(TRIM(COALESCE(txn.ordering_provider_first_name, '')))
	           THEN CONCAT
                    (
                        txn.ordering_provider_last_name, ', ', 
                        txn.ordering_provider_first_name, 
                        CASE WHEN 0 <> LENGTH(TRIM(COALESCE(txn.ordering_provider_middle_initial, ''))) 
                                  THEN CONCAT(' ', txn.ordering_provider_middle_initial) 
                             ELSE '' 
                         END
                    ) 
          ELSE txn.ordering_provider_last_name 
	  END)                                                                              AS ordering_name,
	txn.ordering_provider_specialty                                                       AS ordering_specialty,
	txn.ordering_provider_id                                                              AS ordering_vendor_id,
	txn.ordering_practice_address_line_1                                                    AS ordering_address_1,
	txn.ordering_practice_address_line_2                                                    AS ordering_address_2,
	txn.ordering_practice_city                                                            AS ordering_city,
	VALIDATE_STATE_CODE(txn.ordering_practice_state)                                      AS ordering_state,
	txn.ordering_practice_zipcode                                                         AS ordering_zip
 FROM auroradx_transactions_dedup txn
 LEFT OUTER JOIN auroradx_payload pay
   ON txn.hvJoinKey = pay.hvJoinKey
LEFT JOIN ref_gen_ref esdt
  ON esdt.hvm_vdr_feed_id = 85
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
LEFT JOIN ref_gen_ref ahdt
  ON ahdt.hvm_vdr_feed_id = 85
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
