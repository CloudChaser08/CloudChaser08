SELECT
    txn.rxclaimuid                                                                          AS claim_id,    
    COALESCE(pay.hvid, CONCAT('543_', mbr.memberuid))              AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'177'                                                                                   AS data_feed,
	'543'                                                                                   AS data_vendor,

	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
         	    WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
        	    ELSE NULL
        	END
	    )                                                                                   AS patient_gender,

	/* patient_age (Notes for me - if the age is null this target field will be NULL)*/
	CAP_AGE
	    (
	    VALIDATE_AGE
            (
                pay.age,
                CAST(EXTRACT_DATE(txn.filldate, '%Y-%m-%d') AS DATE),
                COALESCE(mbr.birthyear,pay.yearofbirth)
            )
        )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(txn.filldate, '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    /* patient_zip3 */
   MASK_ZIP_CODE
        (
            CASE
                WHEN 3 = LENGTH(TRIM(COALESCE(mbr.zip3value, ''))) THEN mbr.zip3value
                ELSE pay.threedigitzip
            END
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE(UPPER(COALESCE(mbr.statecode, pay.state, '')))                      AS patient_state,
    /* date_service */
    CASE
        WHEN CAST(TO_DATE(txn.filldate, 'yyyy-MM-dd') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)
          OR CAST(TO_DATE(txn.filldate, 'yyyy-MM-dd') AS DATE) > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
        ELSE CAST(TO_DATE(txn.filldate, 'yyyy-MM-dd') AS DATE)
    END                                                                                      AS date_service,
   /* transaction_code_vendor */
    CASE 
        WHEN txn.claimstatuscode = 'A' THEN 'ADJUSTMENT TO ORIGINAL CLAIM'
        WHEN txn.claimstatuscode = 'D' THEN 'DENIED CLAIMS'
        WHEN txn.claimstatuscode = 'I' THEN 'INITIAL PAY CLAIM'
        WHEN txn.claimstatuscode = 'P' THEN 'PENDED FOR ADJUDICATION'
        WHEN txn.claimstatuscode = 'R' THEN 'REVERSAL TO ORIGINAL CLAIM'
        WHEN txn.claimstatuscode = 'U' THEN 'UNKNOWN'
        ELSE NULL 
    END                                                                                     AS transaction_code_vendor,    
    CLEAN_UP_NDC_CODE(txn.ndc11code)                                                        AS ndc_code,
    txn.dispensedquantity                                                                   AS dispensed_quantity,
    txn.supplydayscount	                                                                    AS days_supply,
---- Mapping added
    CLEAN_UP_NPI_CODE(txn.dispensingnpi)                                                    AS pharmacy_npi, 
---- Mapping removed
--  CLEAN_UP_NPI_CODE(txn.dispensingnpi)                                                    AS prov_dispensing_npi,     
    ---------- Mapping update 2021-01-24  prescribingnpi 2021-02-15   
    CLEAN_UP_NPI_CODE(COALESCE(txn.prescribingnpi, prv.npi1, psp.npinumber))                AS prov_prescribing_npi,    
-----------------------------------------------------------------------------------------
------------------------------------ Name and address
----------------------------------------------------------------------------------------- 
    CASE
        WHEN prv.companyname IS NOT NULL THEN prv.companyname
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename) IS NOT NULL
        THEN        
        	SUBSTR
            (
                CONCAT
                    (
                        CASE WHEN prv.lastname   IS NULL THEN '' ELSE CONCAT(', ', prv.lastname)   END, 
                        CASE WHEN prv.firstname  IS NULL THEN '' ELSE CONCAT(', ', prv.firstname)  END, 
                        CASE WHEN prv.middlename IS NULL THEN '' ELSE CONCAT(', ', prv.middlename) END
                    ), 3
            )
        WHEN psp.name IS NOT NULL THEN psp.name
        ELSE NULL
    END                                                                                     AS prov_prescribing_name_1,
    CASE
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
            THEN prv.primarypracticeaddress
        WHEN psp.name IS NOT NULL 
            THEN psp.address1
    END                                                                                     AS prov_prescribing_address_1,
    CASE
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
            THEN prv.secondarypracticeaddress
        WHEN psp.name IS NOT NULL 
            THEN psp.address2
    END                                                                                     AS prov_prescribing_address_2,
    CASE
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
            THEN prv.practicecity
        WHEN psp.name IS NOT NULL 
            THEN psp.city
    END                                                                                     AS prov_prescribing_city,
    CASE
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
            THEN prv.practicestate
        WHEN psp.name IS NOT NULL 
            THEN psp.state
    END                                                                                     AS prov_prescribing_state,
    CASE
        WHEN COALESCE(prv.lastname, prv.firstname, prv.middlename, prv.companyname) IS NOT NULL
            THEN
                CASE
                    WHEN prv.practicezip IS NOT NULL AND prv.practicezip4 IS NOT NULL
                        THEN CONCAT(COALESCE(prv.practicezip,''),  ' - ', COALESCE(prv.practicezip4,''))
                    WHEN prv.practicezip IS NOT NULL AND prv.practicezip4 IS  NULL
                        THEN prv.practicezip
                    WHEN prv.practicezip IS     NULL AND prv.practicezip4 IS NOT NULL
                        THEN prv.practicezip4
                END
        WHEN psp.name IS NOT NULL 
            THEN psp.zip
    END                                                                                     AS prov_prescribing_zip,
    prv.taxonomycode1                                                                       AS prov_prescribing_std_taxonomy,
    prv.taxonomyspecialization1                                                             AS prov_prescribing_vendor_specialty,
    ---------- Mapping removed 2021-01-24
    -- txn.copayamount                                                                      AS copay_coinsurance,   
    ---------- Mapping update 2021-01-24    
    COALESCE(txn.costamount, txn.allowedamount)                                             AS submitted_gross_due,
    ---------- Mapping removed 2021-01-24 b-- Added 2021-02-14
    rxcc.unadjustedprice                                                                    AS paid_gross_due,
    
	txn.provideruid	                                                                        AS prov_prescribing_id,
	CASE 
	    WHEN txn.provideruid IS NULL THEN NULL
	    ELSE 'Prescriber ID'
	END                                                                                     AS prov_prescribing_qual,
    /* logical_delete_reason */    
    CASE 
        WHEN txn.claimstatuscode = 'R' THEN 'Reversal'
        WHEN txn.claimstatuscode = 'D' THEN 'Claim Rejected'
        WHEN txn.claimstatuscode = 'P' THEN 'Pending'
    ELSE NULL
    END                                                                                     AS  logical_delete_reason,
	'inovalon'                                                                              AS part_provider,

    /* part_best_date */
    CASE
        WHEN 0 = LENGTH(COALESCE
                                (
                                    CAP_DATE
                                           (
                                               CAST(TO_DATE(txn.filldate, 'yyyy-MM-dd') AS DATE),
                                               COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                               CAST('{VDR_FILE_DT}' AS DATE)
                                           ),
                                       ''
                                   ))
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                       SUBSTR(txn.filldate, 1, 4), '-',
                       SUBSTR(txn.filldate, 6, 2), '-01'
                   )
    END                                                                                     AS part_best_date

FROM inovalon_00_dedup txn
LEFT OUTER JOIN (SELECT DISTINCT hvid, claimid, threedigitzip, yearofbirth, gender, age, state from matching_payload) pay   ON txn.memberuid    = pay.claimid
LEFT OUTER JOIN prv    ON txn.provideruid  = prv.provideruid
LEFT OUTER JOIN psp    ON txn.provideruid  = psp.provideruid
LEFT OUTER JOIN mbr    ON txn.memberuid    = mbr.memberuid
-- ---------------Proxy Amount
LEFT OUTER JOIN (SELECT DISTINCT rxclaimuid , rxfilluid FROM rxcw) cw ON txn.rxclaimuid = cw.rxclaimuid
LEFT OUTER JOIN rxcc ON cw.rxfilluid = rxcc.rxfilluid



WHERE lower(txn.rxclaimuid)  <>  'rxclaimuid'
