SELECT
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------Mother
    ----------------------------------------------------------------------------------------------------------------------------- 
    txn.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'01'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'179'                                                                                   AS data_feed,
	'543'                                                                                   AS data_vendor,
    ------------------------------------------------------------------------------------------
    --------------------------------- patient_gender
    ------------------------------------------------------------------------------------------
   CASE
        WHEN SUBSTR(UPPER(txn.gendercode), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gendercode), 1, 1)
        WHEN SUBSTR(UPPER(txn.gender)    , 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(txn.gender)    , 1, 1)
        ELSE NULL
    END                                                                                    AS patient_gender,
    ------------------------------------------------------------------------------------------
    --------------------------------- source_record_date
    ------------------------------------------------------------------------------------------
    CAST(txn.createddate AS DATE)                                                          AS source_record_date,
    CAST(txn.effectivedate AS DATE)                                                        AS date_start,
    CAST(txn.terminationdate AS DATE)                                                      AS date_end,
    txn.benefit_type                                                                       AS benefit_type,
    ------------------------------------------------------------------------------------------
    --------------------------------- Payer Type
    ------------------------------------------------------------------------------------------
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
    ------------------------------------------------------------------------------------------
    --------------------------------- payer_grp_txt
    ------------------------------------------------------------------------------------------
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
                                WHEN txn.acaissuerstatecode IS NULL THEN ''
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
                                                ELSE NULL
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
    ------------------------------------------------------------------------------------------
    --------------------------------- part_best_date
    ------------------------------------------------------------------------------------------
    CASE
        WHEN CAST(txn.effectivedate AS DATE)  < CAST(EXTRACT_DATE('{AVAILABLE_START_DATE}', '%Y-%m-%d') AS DATE)
          OR CAST(txn.effectivedate AS DATE)  > CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)                           THEN '0_PREDATES_HVM_HISTORY'
	    ELSE
	        CONCAT(
	            SUBSTR(
	            	    CAST(txn.effectivedate AS DATE)
	            	   , 1, 7), '-01')
	END                                                                                    AS part_best_date


FROM inv_enr_norm_00 txn
