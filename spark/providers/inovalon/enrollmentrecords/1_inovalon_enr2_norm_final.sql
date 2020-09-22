SELECT /*+ BROADCAST (ref_gen_ref)
 */
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    /* hvid */
    COALESCE
        (
            txn.hvid,
            CONCAT
                (
                    '543_', txn.memberuid
                )
        )                                                                                   AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'03'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'179'                                                                                   AS data_feed,
	'543'                                                                                   AS data_vendor,
	/* patient_age */
	CAP_AGE
	    (
	        VALIDATE_AGE
	            (
	                txn.age,
	                txn.effectivedate,
	                COALESCE
	                    (
	                        txn.birthyear,
	                        txn.yearofbirth
	                    )
	            )
	    )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	/* per AK: null out patient_year_of_birth if YEAR(effectivedate) = patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
            (
                txn.age,
                txn.effectivedate,
                COALESCE(txn.birthyear, txn.yearofbirth)
            )                                                                               AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            CASE
                WHEN 3 = LENGTH(TRIM(COALESCE(txn.zip3value, '')))
                    THEN txn.zip3value
                ELSE txn.threedigitzip
            END
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(txn.statecode, txn.state, ''))
        )                                                                                   AS patient_state,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.gendercode, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.gendercode, 'U'))), 1, 1)
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender, 'U'))), 1, 1)
        	    ELSE 'U'
        	END
	    )                                                                                   AS patient_gender,
	/* source_record_date */
	CAST(txn.createddate AS DATE)                                                           AS source_record_date,
	CASE WHEN CAP_YEAR_OF_BIRTH
            (
                txn.age,
                txn.effectivedate,
                COALESCE(txn.birthyear, txn.yearofbirth)
            ) = YEAR(txn.effectivedate)
        THEN NULL
        ELSE txn.effectivedate
    END                                                                                     AS date_start,
    txn.terminationdate                                                                     AS date_end,
    txn.benefit_type                                                                        AS benefit_type,
    /* payer_plan_name */
    CASE
        WHEN COALESCE
                (
                    txn.productcode,
                    txn.groupplantypecode,
                    txn.acaindicator,
                    txn.acaissuerstatecode,
                    txn.acaonexchangeindicator,
                    txn.acaactuarialvalue
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN txn.productcode IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PRODUCT_CODE: ',
                                            CASE
                                                WHEN txn.productcode = 'E'
                                                    THEN 'EPO'
                                                WHEN txn.productcode = 'F'
                                                    THEN 'PFFS'
                                                WHEN txn.productcode = 'H'
                                                    THEN 'HMO'
                                                WHEN txn.productcode = 'O'
                                                    THEN 'Other'
                                                WHEN txn.productcode = 'P'
                                                    THEN 'PPO'
                                                WHEN txn.productcode = 'S'
                                                    THEN 'POS'
                                                ELSE txn.productcode
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.groupplantypecode IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | GROUP_PLAN_TYPE_CODE: ',
                                            CASE
                                                WHEN txn.groupplantypecode = 'ID'
                                                    THEN 'Individual'
                                                WHEN txn.groupplantypecode = 'SM'
                                                    THEN 'Small'
                                                WHEN txn.groupplantypecode = 'LG'
                                                    THEN 'Large'
                                                ELSE txn.groupplantypecode
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.acaindicator IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_INDICATOR: ',
                                            CASE
                                                WHEN txn.acaindicator = '0'
                                                    THEN 'No'
                                                WHEN txn.acaindicator = '1'
                                                    THEN 'Yes'
                                                ELSE txn.acaindicator
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.acaissuerstatecode IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ISSUER_STATE_CODE: ',
                                            txn.acaissuerstatecode
                                        )
                            END,
                            CASE
                                WHEN txn.acaonexchangeindicator IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ON_EXCHANGE_INDICATOR: ',
                                            CASE
                                                WHEN txn.acaonexchangeindicator = '0'
                                                    THEN 'No'
                                                WHEN txn.acaonexchangeindicator = '1'
                                                    THEN 'Yes'
                                                ELSE txn.acaonexchangeindicator
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.acaactuarialvalue IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ACTUARIAL_VALUE: ',
                                            txn.acaactuarialvalue,
                                            '%'
                                        )
                            END
                        ), 4
                )
    END                                                                                     AS payer_plan_name,
    /* payer_type */
    CASE
        WHEN txn.payergroupcode IS NULL
            THEN NULL
        WHEN txn.payergroupcode = 'C'
            THEN 'Commercial'
        WHEN txn.payergroupcode = 'D'
            THEN 'Dual Eligible'
        WHEN txn.payergroupcode = 'M'
            THEN 'Medicaid'
        WHEN txn.payergroupcode = 'R'
            THEN 'Medicare Advantage'
        WHEN txn.payergroupcode = 'U'
            THEN 'Unknown'
        ELSE txn.payergroupcode
    END                                                                                     AS payer_type,
    'inovalon'                                                                              AS part_provider,
    /* part_best_date */
    /* bpm - capping was not included: */

    CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            txn.effectivedate,
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),  '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    WHEN CAP_YEAR_OF_BIRTH
            (
                txn.age,
                txn.effectivedate,
                COALESCE(txn.birthyear, txn.yearofbirth)
            ) = YEAR(txn.effectivedate)
            THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT(SUBSTR(TRIM(txn.effectivedate), 1, 7), '-01')
	END                                                                                     AS part_best_date 
 FROM inovalon_enr2_norm_enr txn
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 179
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 179
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
