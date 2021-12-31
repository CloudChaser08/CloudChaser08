SELECT
--    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.row_level_id                                                                        AS claim_id,
    pay.hvid                                                                                AS hvid,
    CAST(CURRENT_DATE() AS STRING)                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'259'                                                                                   AS data_feed,
	'1834'                                                                                  AS data_vendor,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     Gender                 -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
	(
    	CASE
    	    WHEN SUBSTR(UPPER(txn.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gender), 1, 1)
    	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender), 1, 1)
    	    WHEN pay.gender IS NOT NULL OR  txn.gender IS NOT NULL THEN 'U'
    	ELSE NULL
    	END
    )                                                                                       AS patient_gender,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     Age                    -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
	CAP_AGE -- Convert age values >= 85 to 90
	    (
	    VALIDATE_AGE  -- Ensure that the age is within 2 years of its derivation from date_service and year_of_birth.
            (
                pay.age,
                CAST(EXTRACT_DATE(txn.dispense_status_date, '%Y%m%d') AS DATE),
                pay.yearofbirth
            )
        )                                                                                   AS patient_age,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     patient_year_of_birth  -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(txn.dispense_status_date, '%Y%m%d') AS DATE), pay.yearofbirth),
            CAST(EXTRACT_DATE(txn.dispense_status_date, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                    AS patient_year_of_birth ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     patient_year_of_birth  -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                           AS patient_zip3          ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     State                  -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    VALIDATE_STATE_CODE(UPPER(pay.state))                                                    AS patient_state         ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     Service Date           -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) THEN
            CASE
                WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                  OR CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
            ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE)
            END
    ELSE
            CASE
                WHEN CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                  OR CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
            ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE)
            END
    END                                                                                      AS date_service          ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     date_written           -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.prescription_written_date, 1, 8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(SUBSTR(txn.prescription_written_date, 1, 8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.prescription_written_date, 1, 8), '%Y%m%d') AS DATE)
    END                                                                                      AS date_written          ,
    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     date_authorized        -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.prior_authorization_effective_date, 1, 8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(SUBSTR(txn.prior_authorization_effective_date, 1, 8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
    ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.prior_authorization_effective_date, 1, 8), '%Y%m%d') AS DATE)
    END                                                                                      AS date_authorized       ,

    ----------------------------------------------------------------------------------------------------------------------------
    -----------------------------------------     diagnosis_code         -------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_DIAGNOSIS_CODE
            (pvt.diagnosis_code,
             pvt.diagnosis_code_qual,
            CASE
                WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) THEN
                    CASE
                        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                          OR CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                    ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE)
                    END
            ELSE
                CASE
                    WHEN CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                      OR CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE)
                END
            END
            )                                                                                   AS diagnosis_code ,
    pvt.diagnosis_code_qual                                                                     AS diagnosis_code_qual  ,
    ----------------------------------------------------------------------------------------------------------------------------

    CLEAN_UP_NDC_CODE(txn.prescription_ndc)                                                     AS ndc_code              ,
    txn.prescription_number                                                                     AS rx_number             ,
	(
	CASE
	    WHEN txn.prescription_number IS NOT NULL THEN
        	CASE
        	    WHEN txn.prescription_type ='N' THEN 'RX_NUMBER_NEW'
        	    WHEN txn.prescription_type ='R' THEN 'RX_NUMBER_REFILL'
        	    ELSE NULL
        	END
    ELSE NULL
    END
    )                                                                                           AS rx_number_qual        ,

    pvt.bin_number,
    pvt.processor_control_number,

    CAST(txn.total_prescription_refills_allowed AS INTEGER) - CAST(txn.refills_remaining AS INTEGER) AS fill_number      ,
    txn.total_prescription_refills_allowed                                                      AS refill_auth_amount    ,
    CAST(txn.dispense_quantity AS FLOAT)                                                        AS dispensed_quantity    ,
    CAST(txn.days_supply       AS INTEGER)                                                      AS days_supply           ,
    CLEAN_UP_NPI_CODE(txn.pharmacy_npi_number)                                                  AS pharmacy_npi          ,

    pvt.payer_id,
    pvt.payer_id_qual,

    CLEAN_UP_NPI_CODE(txn.prescriber_npi_number)                                                AS prov_prescribing_npi  ,
    CASE
        WHEN txn.primary_pharmacy_payor_id_code   IS NOT NULL
         AND txn.secondary_pharmacy_payor_id_code IS NOT NULL THEN 2
        WHEN txn.primary_pharmacy_payor_id_code   IS NOT NULL THEN 1
        WHEN txn.secondary_pharmacy_payor_id_code IS NOT NULL THEN 1
    ELSE NULL
    END                                                                                         AS cob_count             ,

    txn.patient_assistance_amount                                                               AS other_payer_recognized,
    pvt.remaining_deductible,
    pvt.periodic_benefit_exceed,
    pvt.copay_coinsurance,
    CASE
        WHEN txn.patient_assistance_source = 'N' THEN NULL
        ELSE txn.patient_assistance_source
    END                                                                                         AS other_payer_coverage_id,

    'acentrus'                                                                                  AS part_provider         ,
    -- ----------------------------------------------------------------------------------------------------------------------------
    -- -----------------------------------------     part_best_date         -------------------------------------------------------
    -- ----------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN 0 = LENGTH(COALESCE
                                (
                                    CAP_DATE
                                          (
                                             CASE
                                                WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) THEN
                                                    CASE
                                                        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                                                          OR CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                    ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.shipping_date,1,8), '%Y%m%d') AS DATE)
                                                    END
                                            ELSE
                                                    CASE
                                                        WHEN CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
                                                          OR CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
                                                    ELSE     CAST(EXTRACT_DATE(SUBSTR(txn.dispense_status_date, 1,8), '%Y%m%d') AS DATE)
                                                    END
                                            END ,
                                            COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                          ),
                                      ''
                                  ))
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE
                (
                    CASE
                        WHEN CAST(EXTRACT_DATE(txn.shipping_date, '%Y%m%d') AS DATE) > CAST(EXTRACT_DATE(txn.dispense_status_date, '%Y%m%d') AS DATE) THEN
                                CONCAT(SUBSTR(txn.shipping_date, 1, 4), '-', SUBSTR(txn.shipping_date, 5, 2), '-01')
                    ELSE
                                CONCAT(SUBSTR(txn.dispense_status_date, 1, 4), '-', SUBSTR(txn.dispense_status_date, 5, 2), '-01')
                    END
                )
    END                                                                                        AS part_best_date


FROM acentrus_norm_00_dedup txn
LEFT OUTER JOIN matching_payload  pay                     ON TRIM(UPPER(txn.row_level_id)) = TRIM(UPPER(pay.claimid))
LEFT OUTER JOIN acentrus_norm_20_update_pivot  pvt  ON TRIM(UPPER(txn.row_level_id)) = TRIM(UPPER(pvt.claim_id))
---------------
WHERE TRIM(UPPER(COALESCE(txn.row_level_id, 'empty')))  <> 'ROW LEVEL ID'
AND txn.dispense_status NOT IN ('RP19', 'RP18')
--limit 1