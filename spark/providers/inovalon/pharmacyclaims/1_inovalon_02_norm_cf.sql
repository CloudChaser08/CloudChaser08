SELECT
    rxc.rxclaimuid                                                                          AS claim_id,    
    COALESCE(pay.hvid, CONCAT('543_', mbr.memberuid))                                       AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(rxc.input_file_name, '/')[SIZE(SPLIT(rxc.input_file_name, '/')) - 1]              AS data_set,
	'177'                                                                                   AS data_feed,
	'543'                                                                                   AS data_vendor,

	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(mbr.gendercode), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(mbr.gendercode), 1, 1)
        	    WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender,

	/* patient_age (Notes for me - if the age is null this target field will be NULL)*/
	CAP_AGE
	    (
	    VALIDATE_AGE
            (
                pay.age,
                CAST(EXTRACT_DATE(rxc.filldate, '%Y-%m-%d') AS DATE),
                COALESCE(mbr.birthyear,pay.yearofbirth)
            )
        )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(EXTRACT_DATE(rxc.filldate, '%Y-%m-%d') AS DATE),
            COALESCE(mbr.birthyear,pay.yearofbirth)
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
	CAP_DATE
        (
            CAST(EXTRACT_DATE(rxc.filldate, '%Y-%m-%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
   /* transaction_code_vendor */
    CASE 
        WHEN rxc.claimstatuscode = 'A' THEN 'ADJUSTMENT TO ORIGINAL CLAIM'
        WHEN rxc.claimstatuscode = 'D' THEN 'DENIED CLAIMS'
        WHEN rxc.claimstatuscode = 'I' THEN 'INITIAL PAY CLAIM'
        WHEN rxc.claimstatuscode = 'P' THEN 'PENDED FOR ADJUDICATION'
        WHEN rxc.claimstatuscode = 'R' THEN 'REVERSAL TO ORIGINAL CLAIM'
        WHEN rxc.claimstatuscode = 'U' THEN 'UNKNOWN'
        ELSE NULL 
    END                                                                                     AS transaction_code_vendor,    
    CLEAN_UP_NDC_CODE(rxc.ndc11code)                                                        AS ndc_code,
    rxc.dispensedquantity                                                                   AS dispensed_quantity,
    rxc.supplydayscount	                                                                    AS days_supply,
    CLEAN_UP_NPI_CODE(rxc.dispensingnpi)                                                    AS prov_dispensing_npi, 
    CLEAN_UP_NPI_CODE(COALESCE(rxc.dispensingnpi, prv.npi1))                                AS prov_prescribing_npi,    
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
    rxc.copayamount                                                                         AS copay_coinsurance,      
    COALESCE( rxc.billedamount, rxc.costamount, rxc.allowedamount)                          AS submitted_gross_due,
    rxc.paidamount                                                                          AS paid_gross_due,
	rxc.provideruid	                                                                        AS prov_prescribing_id,
	CASE 
	    WHEN rxc.provideruid IS NULL THEN NULL
	    ELSE 'Prescriber ID'
	END                                                                                     AS prov_prescribing_qual,
    /* logical_delete_reason */    
    CASE 
        WHEN rxc.claimstatuscode = 'R' THEN 'Reversal'
        WHEN rxc.claimstatuscode = 'D' THEN 'Claim Rejected'
        WHEN rxc.claimstatuscode = 'P' THEN 'Pending'
    ELSE NULL
    END                                                                                     AS  logical_delete_reason,
                
	'inovalon'                                                                              AS part_provider,

    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(rxc.filldate, '%Y-%m-%d') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(rxc.filldate, 1, 4), '-',
                    SUBSTR(rxc.filldate, 6, 2), '-01'
                )
	END                                                                                     AS part_best_date 
	
FROM rxc
LEFT OUTER JOIN matching_payload pay ON rxc.memberuid    = pay.claimid
LEFT OUTER JOIN prv ON rxc.provideruid  = prv.provideruid
LEFT OUTER JOIN psp ON rxc.provideruid  = psp.provideruid
LEFT OUTER JOIN mbr ON rxc.memberuid    = mbr.memberuid
WHERE lower(rxc.rxclaimuid)  <>  'rxclaimuid'
