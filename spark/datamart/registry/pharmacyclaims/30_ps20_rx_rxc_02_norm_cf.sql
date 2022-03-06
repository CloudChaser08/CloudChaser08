SELECT
    rxc.rxclaimuid                                                                         AS claim_id,
    pay.hvid                                                                               AS hvid,
    CURRENT_DATE()                                                                         AS created,
	'1'                                                                                    AS model_version,
    SPLIT(rxc.input_file_name, '/')[SIZE(SPLIT(rxc.input_file_name, '/')) - 1]             AS data_set,
	'177'                                                                                  AS data_feed,
	'543'                                                                                  AS data_vendor,
    CASE
        WHEN SUBSTR(UPPER(mbr.gendercode), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(mbr.gendercode), 1, 1)
        WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
        ELSE NULL
    END                                                                                    AS patient_gender,
    pay.yearofbirth                                                                      AS patient_year_of_birth,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ date_service
    -----------------------------------------------------------------------------------------------------------------------------
     CAP_DATE
        (
            CAST(EXTRACT_DATE(rxc.filldate                    , '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{EARLIEST_SERVICE_DATE}', '%Y-%m-%d') AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}'                , '%Y-%m-%d') AS DATE)
        )

                                                                                 AS date_service,
    -----------------------------------------------------------------------------------------------------------------------------
    ------------------------------------ transaction_code_vendor
    -----------------------------------------------------------------------------------------------------------------------------
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
    ----- Added 2022-03-01
    prv.taxonomycode1                                                                       AS prov_prescribing_std_taxonomy,
--    rxcc.unadjustedprice	                                                                AS submitted_gross_due,
    MD5(rxc.dispensingnpi)                                                                  AS prov_dispensing_id,
	CASE
	    WHEN rxc.dispensingnpi IS NULL THEN NULL
	    ELSE 'Dispensing ID'
	END                                                                                     AS prov_dispensing_qual,

    MD5(COALESCE(rxc.prescribingnpi, prv.npi1, psp.npinumber))                             AS prov_prescribing_id,
	CASE
	    WHEN rxc.dispensingnpi IS NULL THEN NULL
	    ELSE 'Prescribing ID'
	END                                                                                     AS prov_prescribing_qual,


    /* logical_delete_reason */
    CASE
        WHEN rxc.claimstatuscode = 'R' THEN 'Reversal'
        WHEN rxc.claimstatuscode = 'D' THEN 'Claim Rejected'
        WHEN rxc.claimstatuscode = 'P' THEN 'Pending'
    ELSE NULL
    END                                                                                     AS  logical_delete_reason,
    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE) AS stg_file_date,
	'inovalon'                                                                              AS part_provider,

     	CASE
    	    WHEN 0 = LENGTH(COALESCE
    	                            (
    	                                CAP_DATE
                                            (
                                                CAST(EXTRACT_DATE(rxc.filldate, '%Y-%m-%d') AS DATE),
                                                CAST(EXTRACT_DATE(COALESCE('{AVAILABLE_START_DATE}','{EARLIEST_SERVICE_DATE}'), '%Y-%m-%d') AS DATE),
                                                CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                            )
                                     , '')
                            )
    	        THEN '0_PREDATES_HVM_HISTORY'
    	    ELSE CONCAT
    	            (
                        SUBSTR(rxc.filldate, 1, 4), '-',
                        SUBSTR(rxc.filldate, 6, 2), '-01'
                     )
     	END         AS part_best_date


FROM ps20_rx_rxc_00_dedup rxc
--LEFT OUTER JOIN (SELECT DISTINCT hvid, claimid, threedigitzip, yearofbirth, gender, age, state from matching_payload) pay   ON rxc.memberuid    = pay.claimid
INNER JOIN matching_payload pay   ON rxc.memberuid    = pay.claimid
LEFT OUTER JOIN prv    ON rxc.provideruid  = prv.provideruid
LEFT OUTER JOIN psp    ON rxc.provideruid  = psp.provideruid
LEFT OUTER JOIN mbr    ON rxc.memberuid    = mbr.memberuid

-- ---------------Proxy Amount
-- LEFT OUTER JOIN (SELECT DISTINCT rxclaimuid , rxfilluid FROM inovalon_rxcw) rxcw ON rxc.rxclaimuid = rxcw.rxclaimuid
-- LEFT OUTER JOIN inovalon_rxcc        rxcc ON rxcw.rxfilluid = rxcc.rxfilluid

INNER JOIN _mom_cohort   mom ON pay.hvid = mom.hvid  ----------- Cohort has both mom and baby TEMPORARY


WHERE lower(rxc.rxclaimuid)  <>  'rxclaimuid'
