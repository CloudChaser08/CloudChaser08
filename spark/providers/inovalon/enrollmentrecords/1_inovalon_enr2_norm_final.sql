SELECT
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
	'06'                                                                                    AS model_version,
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
                    txn.yearofbirth
	            )
	    )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	/* per AK: null out patient_year_of_birth if YEAR(effectivedate) = patient_year_of_birth */
    ------------------------------------------------------------------------------------------
    -- txn.birthyear is removed and ONLY yearofbirth from PAYLOAD is been considered 2021-02-11
    ------------------------------------------------------------------------------------------    
	CASE
	    WHEN  YEAR(effectivedate) =  txn.yearofbirth THEN NULL
	ELSE
    	CAP_YEAR_OF_BIRTH
                (
                    txn.age,
                    txn.effectivedate,
                    txn.yearofbirth
                )                                                                               
    END                                                                                     AS patient_year_of_birth,
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
        	    WHEN txn.gender IN ('F', 'M', 'U') THEN txn.gender
        	    ELSE NULL 
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
    ------------------------Payer Type
    CASE
        WHEN  txn.payergroupcode IS NULL THEN NULL
        ELSE
            CASE
                WHEN txn.payergroupcode = 'C' THEN 'Commercial' 
                WHEN txn.payergroupcode = 'D' THEN 'Dual Eligible' 
                WHEN txn.payergroupcode = 'M' THEN 'Medicaid' 
                WHEN txn.payergroupcode = 'R' THEN 'Medicare Advantage' 
                WHEN txn.payergroupcode = 'U' THEN 'Unknown' 
            ELSE txn.payergroupcode
            END
        END                                                                                 AS payer_type,
    /* payer_grp_txt */
    CASE
        WHEN COALESCE
                (
                    txn.payertypecode,
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
                            ' | PAYER_SUB_TYPE_CODE: ',
                            CASE
                                WHEN UPPER(txn.payertypecode) = 'C'  THEN 'Commercial'
                                WHEN UPPER(txn.payertypecode) = 'CM' THEN 'Commercial and Medicaid'
                                WHEN UPPER(txn.payertypecode) = 'CR' THEN 'Commercial and Medicare' 
                                WHEN UPPER(txn.payertypecode) = 'CS' THEN 'Commercial and SNP'
                                WHEN UPPER(txn.payertypecode) = 'D'  THEN 'Dual Eligible'
                                WHEN UPPER(txn.payertypecode) = 'F'  THEN 'Family Care'
                                WHEN UPPER(txn.payertypecode) = 'H'  THEN 'CHIP'
                                WHEN UPPER(txn.payertypecode) = 'K'  THEN 'Marketplace'
                                WHEN UPPER(txn.payertypecode) = 'M'  THEN 'Medicaid'
                                WHEN UPPER(txn.payertypecode) = 'MD' THEN 'Medicaid Disabled'
                                WHEN UPPER(txn.payertypecode) = 'ML' THEN 'Medicaid Low Income' 
                                WHEN UPPER(txn.payertypecode) = 'MR' THEN 'Medicaid Restricted' 
                                WHEN UPPER(txn.payertypecode) = 'NC' THEN 'Special Needs Plan - Chronic Condition'
                                WHEN UPPER(txn.payertypecode) = 'ND' THEN 'Special Needs Plan - Dual Eligible'
                                WHEN UPPER(txn.payertypecode) = 'NI' THEN 'Special Needs Plan - Institutionalized'
                                WHEN UPPER(txn.payertypecode) = 'NM' THEN 'Special Needs Plan- Medicaid only'
                                WHEN UPPER(txn.payertypecode) = 'NR' THEN 'Special Needs Plan - Medicare only' 
                                WHEN UPPER(txn.payertypecode) = 'O'  THEN 'Other'
                                WHEN UPPER(txn.payertypecode) = 'R'  THEN 'Medicare' 
                                WHEN UPPER(txn.payertypecode) = 'RC' THEN 'Medicare Cost' 
                                WHEN UPPER(txn.payertypecode) = 'RM' THEN 'Medicare - Medicaid' 
                                WHEN UPPER(txn.payertypecode) = 'RR' THEN 'Medicare Risk'
                                WHEN UPPER(txn.payertypecode) = 'S'  THEN 'Self Insured'
                            ELSE txn.payertypecode
                            END,                        
                            CASE
                                WHEN txn.productcode IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PRODUCT_CODE: ',
                                            CASE
                                                WHEN txn.productcode = 'E' THEN 'EPO'
                                                WHEN txn.productcode = 'F' THEN 'PFFS'
                                                WHEN txn.productcode = 'H' THEN 'HMO'
                                                WHEN txn.productcode = 'O' THEN 'Other'
                                                WHEN txn.productcode = 'P' THEN 'PPO'
                                                WHEN txn.productcode = 'S' THEN 'POS'
                                            ELSE txn.productcode
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.groupplantypecode IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | GROUP_PLAN_TYPE_CODE: ',
                                            CASE
                                                WHEN txn.groupplantypecode = 'ID' THEN 'Individual'
                                                WHEN txn.groupplantypecode = 'SM' THEN 'Small'
                                                WHEN txn.groupplantypecode = 'LG' THEN 'Large'
                                            ELSE txn.groupplantypecode
                                            END
                                        )
                            END,
                            CASE
                                WHEN txn.acaindicator IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_INDICATOR: ',
                                            CASE
                                                WHEN txn.acaindicator = '0' THEN 'No'
                                                WHEN txn.acaindicator = '1' THEN 'Yes'
                                            ELSE NULL -- txn.acaindicator
                                            END
                                        )
                            END,
                            CASE
                                WHEN VALIDATE_STATE_CODE(txn.acaissuerstatecode) IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ISSUER_STATE_CODE: ',
                                            txn.acaissuerstatecode
                                        )
                            END,
                            CASE
                                WHEN txn.acaonexchangeindicator IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ON_EXCHANGE_INDICATOR: ',
                                            CASE
                                                WHEN txn.acaonexchangeindicator = '0'
                                                    THEN 'No'
                                                WHEN txn.acaonexchangeindicator = '1'
                                                    THEN 'Yes'
                                                ELSE NULL -- txn.acaonexchangeindicator
                                            END
                                        )
                            END,
                            CASE
                                WHEN CAST(txn.acaactuarialvalue AS FLOAT) IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | ACA_ACTUARIAL_VALUE: ',
                                            txn.acaactuarialvalue,
                                            '%'
                                        )
                            END
                        ), 4
                )
    END                                                                                     AS payer_grp_txt,
    'inovalon'                                                                              AS part_provider,
    /* part_best_date */
    /* bpm - capping was not included: */
    
    CASE
        WHEN CAST(txn.effectivedate AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
            OR CAST(txn.effectivedate AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)                           THEN '0_PREDATES_HVM_HISTORY'
	    WHEN CAP_YEAR_OF_BIRTH
            (
                txn.age,
                txn.effectivedate,
                txn.yearofbirth
            ) = YEAR(txn.effectivedate)                                                   THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT(SUBSTR(TRIM(txn.effectivedate), 1, 7), '-01')
	END                                                                                     AS part_best_date 
 FROM inovalon_enr2_norm_enr txn
