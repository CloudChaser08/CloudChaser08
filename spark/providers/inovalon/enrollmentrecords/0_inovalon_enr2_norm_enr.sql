SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate                                                                   AS orig_effectivedate,
    enr.terminationdate                                                                 AS orig_terminationdate,
    /* effectivedate */
    CASE
        WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)
            THEN NULL
        WHEN enr.effectivedate < CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)
            THEN NULL
        ELSE enr.effectivedate
    END                                                                                 AS effectivedate,
    /* terminationdate */
    CASE
        WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)
            THEN NULL
        WHEN enr.effectivedate < CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)
            THEN NULL
        WHEN enr.terminationdate > CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
            THEN CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
        ELSE enr.terminationdate
    END                                                                                 AS terminationdate,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
    enr.payertypecode,
    enr.input_file_name,
	mbr.statecode,
    mbr.birthyear,
    mbr.zip3value,
    mbr.gendercode,
	pay.age,
    pay.state,
    pay.hvid,
    pay.yearofbirth,
    pay.threedigitzip,
    pay.gender,
	'MEDICAL'                                                                           AS benefit_type
 FROM enr
 LEFT OUTER JOIN mbr
              ON enr.memberuid = mbr.memberuid
 LEFT OUTER JOIN matching_payload pay
              ON enr.memberuid = pay.claimid
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(medicalindicator, 'X') = '1'
UNION ALL
SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate                                                                   AS orig_effectivedate,
    enr.terminationdate                                                                 AS orig_terminationdate,
    /* effectivedate */
    CASE
        WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)
            THEN NULL
        WHEN enr.effectivedate < CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)
            THEN NULL
        ELSE enr.effectivedate
    END                                                                                 AS effectivedate,
    /* terminationdate */
    CASE
        WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)
            THEN NULL
        WHEN enr.effectivedate < CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)
            THEN NULL
        WHEN enr.terminationdate > CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
            THEN CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
        ELSE enr.terminationdate
    END                                                                                 AS terminationdate,
    enr.createddate,
    enr.productcode,
    enr.groupplantypecode,
    enr.acaindicator,
    enr.acaissuerstatecode,
    enr.acaonexchangeindicator,
    enr.acaactuarialvalue,
    enr.payergroupcode,
    enr.payertypecode,
    enr.input_file_name,
	mbr.statecode,
    mbr.birthyear,
    mbr.zip3value,
    mbr.gendercode,
	pay.age,
    pay.state,
    pay.hvid,
    pay.yearofbirth,
    pay.threedigitzip,
    pay.gender,
	'PHARMACY'                                                                          AS benefit_type
FROM enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid
WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(rxindicator, 'X') = '1'
