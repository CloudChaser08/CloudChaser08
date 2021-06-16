SELECT
    sln.claim_number                                                         AS claimid,
    sln.record_type                                                          AS recordtype,
    CAST(sln.line_number AS INT)                                             AS linenumber,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_from,   '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.statement_from, '%Y%m%d' ) AS DATE)
    )                                                                       AS servicefromdate,
    COALESCE
    (
        CAST(EXTRACT_DATE(sln.service_to,    '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.statement_to,  '%Y%m%d' ) AS DATE)
    )                                                                       AS servicetodate,
    CASE 
        WHEN    sln.place_service IS NULL THEN NULL
        WHEN    COALESCE(clm.claim_type_code, 'X') <> 'P'   THEN NULL
        WHEN    LPAD(sln.place_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN '99' 
        ELSE    LPAD(sln.place_service , 2, '0') 
    END                                                                      AS placeofserviceid,
    CLEAN_UP_ALPHANUMERIC_CODE
    (
         UPPER(sln.procedure)
    )                                                                       AS procedurecode,
	CASE
	    WHEN sln.procedure IS NULL
	         THEN NULL
	    ELSE sln.procedure_qual
	END                                                                     AS procedurecodequal,	
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.procedure_modifier_1)),1 ,2)  AS proceduremodifier1, 
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.procedure_modifier_2)),1 ,2)  AS proceduremodifier2, 
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.procedure_modifier_3)),1 ,2)  AS proceduremodifier3, 
    SUBSTR(CLEAN_UP_ALPHANUMERIC_CODE(UPPER(sln.procedure_modifier_4)),1 ,2)  AS proceduremodifier4, 
    CAST(sln.line_charge AS FLOAT)	                                          AS linecharges,
    CAST(sln.units AS FLOAT)                                                  AS unitcount,
    sln.revenue_code                                                          AS revenuecode,
    sln.diagnosis_pointer_1                         	                      AS diagnosiscodepointer1,
    sln.diagnosis_pointer_2                         	                      AS diagnosiscodepointer2,
    sln.diagnosis_pointer_3                         	                      AS diagnosiscodeoointer3,
    sln.diagnosis_pointer_4                         	                      AS diagnosiscodepointer4,
    CLEAN_UP_NUMERIC_CODE(sln.ndc)                                            AS ndccode,
    CASE 
        WHEN SUBSTR(UPPER(sln.emergency), 1, 1) IN ('Y', 'N')  THEN SUBSTR(UPPER(sln.emergency), 1, 1)
        ELSE 'N' 
    END                                                                   AS emergencyind,   
    CASE
        WHEN clm.claim_type_code = 'I' THEN 'NelsonInst' 
        WHEN clm.claim_type_code = 'P' THEN 'NelsonProf'
    END                                                                  AS data_vendor,
    CAST(NULL AS STRING)                                                  AS dhcreceiveddate,
    CAST(NULL AS STRING)                                                    AS rownumber,
    sln.data_set                                                          AS sourcefilename,
    CAST(NULL AS INT)                                                  AS fileyear,
    CAST(NULL AS INT)                                                  AS filemonth,
    CAST(NULL AS INT)                                                  AS fileday,
    '24'                                                                  AS data_feed,
    'navicure'                                                             AS part_provider,
    CASE
	    WHEN COALESCE
            (
	           CAST(EXTRACT_DATE(sln.service_from,    '%Y%m%d' ) AS DATE), 
               CAST(EXTRACT_DATE(clm.statement_from,  '%Y%m%d' ) AS DATE)
	        ) IS NULL   THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	       (
	           SUBSTR(COALESCE(sln.service_from, clm.statement_from), 1, 4), '-',
	           SUBSTR(COALESCE(sln.service_from, clm.statement_from), 5, 2), '-01'
	       )
	END                                                                  AS part_best_date

 FROM waystar_dedup_lines sln 
 LEFT OUTER JOIN waystar_dedup_claims clm 
   ON clm.claim_number = sln.claim_number 
WHERE UPPER(COALESCE(clm.claim_type_code, 'X')) in ('P', 'I') 
/* Eliminate claims loaded from Navicure source data.*/
AND NOT EXISTS        
    (
        SELECT 1
         FROM waystar_medicalclaims_augment_comb aug
        WHERE sln.claim_number  = aug.instanceid
    ) 
ORDER BY sln.claim_number
