SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate                                                                   AS orig_effectivedate,
    enr.terminationdate                                                                 AS orig_terminationdate,
    --------------------------------------------------------------------------------------------------------------
    -----------------------------------New Logic introduced (JKS - 2021-08-17) modified code 2021-09-20 ----------
    --------------------------------------------------------------------------------------------------------------
    -------------------Latest date is the start date -------------------------------------------------------------
    CASE
        WHEN enr.effectivedate IS NULL                              AND  enr.claimadjustedeffectivedate IS NULL AND enr.rxclaimadjustedeffectivedate IS NULL THEN NULL
        WHEN enr.effectivedate IS NOT NULL                          AND  enr.claimadjustedeffectivedate IS NULL AND enr.rxclaimadjustedeffectivedate IS NULL THEN enr.effectivedate

        WHEN COALESCE(enr.effectivedate, '1900-01-01') >= COALESCE(enr.claimadjustedeffectivedate, '1900-01-01')   AND COALESCE(enr.effectivedate , '1900-01-01') >= COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01')
                        THEN enr.effectivedate
        WHEN COALESCE(enr.claimadjustedeffectivedate , '1900-01-01') >= COALESCE(enr.effectivedate, '1900-01-01')  AND COALESCE(enr.claimadjustedeffectivedate  ,'1900-01-01')  >= COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01')
                        THEN enr.claimadjustedeffectivedate
        WHEN COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01') >= COALESCE(enr.effectivedate, '1900-01-01') AND COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01') >= COALESCE(enr.claimadjustedeffectivedate, '1900-01-01')
                        THEN enr.rxclaimadjustedeffectivedate
        ELSE NULL
    END                                                                                  AS effectivedate,
    -------------------Earliest date is the terminate date -------------------------------------------------------
    CASE
        WHEN enr.terminationdate IS NULL                           AND  enr.claimadjustedterminationdate IS NULL AND enr.rxclaimadjustedterminationdate IS NULL THEN NULL
        WHEN enr.terminationdate IS NOT NULL                       AND  enr.claimadjustedterminationdate IS NULL AND enr.rxclaimadjustedterminationdate IS NULL THEN enr.terminationdate

        WHEN COALESCE(enr.terminationdate, '9999-01-01') <= COALESCE(claimadjustedterminationdate, '9999-01-01')   AND COALESCE(enr.terminationdate, '9999-01-01') <= COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01')
                        THEN enr.terminationdate
        WHEN COALESCE(enr.claimadjustedterminationdate, '9999-01-01')   <= COALESCE(terminationdate, '9999-01-01') AND COALESCE(enr.claimadjustedterminationdate, '9999-01-01') <= COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01')
                        THEN enr.claimadjustedterminationdate
        WHEN COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01') <= COALESCE(terminationdate, '9999-01-01') AND COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01') <= COALESCE(enr.terminationdate, '9999-01-01')
                        THEN enr.rxclaimadjustedterminationdate
        ELSE NULL
    END                                                                                  AS terminationdate,
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
 FROM enr_adj enr
LEFT OUTER JOIN mbr mbr ON enr.memberuid = mbr.memberuid
LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid

WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(medicalindicator, 'X') = '1'
UNION ALL
SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate                                                                   AS orig_effectivedate,
    enr.terminationdate                                                                 AS orig_terminationdate,
    --------------------------------------------------------------------------------------------------------------
    -----------------------------------New Logic introduced (JKS - 2021-08-17)------------------------------------
    -------------------Latest date is the start date -------------------------------------------------------------
    CASE
        WHEN enr.effectivedate IS NULL                              AND  enr.claimadjustedeffectivedate IS NULL AND enr.rxclaimadjustedeffectivedate IS NULL THEN NULL
        WHEN enr.effectivedate IS NOT NULL                          AND  enr.claimadjustedeffectivedate IS NULL AND enr.rxclaimadjustedeffectivedate IS NULL THEN enr.effectivedate

        WHEN COALESCE(enr.effectivedate, '1900-01-01') >= COALESCE(enr.claimadjustedeffectivedate, '1900-01-01')   AND COALESCE(enr.effectivedate , '1900-01-01') >= COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01')
                        THEN enr.effectivedate
        WHEN COALESCE(enr.claimadjustedeffectivedate , '1900-01-01') >= COALESCE(enr.effectivedate, '1900-01-01')  AND COALESCE(enr.claimadjustedeffectivedate  ,'1900-01-01')  >= COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01')
                        THEN enr.claimadjustedeffectivedate
        WHEN COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01') >= COALESCE(enr.effectivedate, '1900-01-01') AND COALESCE(enr.rxclaimadjustedeffectivedate, '1900-01-01') >= COALESCE(enr.claimadjustedeffectivedate, '1900-01-01')
                        THEN enr.rxclaimadjustedeffectivedate
        ELSE NULL
    END                                                                                  AS effectivedate,
    -------------------Earliest date is the terminate date -------------------------------------------------------
    CASE
        WHEN enr.terminationdate IS NULL                           AND  enr.claimadjustedterminationdate IS NULL AND enr.rxclaimadjustedterminationdate IS NULL THEN NULL
        WHEN enr.terminationdate IS NOT NULL                       AND  enr.claimadjustedterminationdate IS NULL AND enr.rxclaimadjustedterminationdate IS NULL THEN enr.terminationdate

        WHEN COALESCE(enr.terminationdate, '9999-01-01') <= COALESCE(claimadjustedterminationdate, '9999-01-01')   AND COALESCE(enr.terminationdate, '9999-01-01') <= COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01')
                        THEN enr.terminationdate
        WHEN COALESCE(enr.claimadjustedterminationdate, '9999-01-01')   <= COALESCE(terminationdate, '9999-01-01') AND COALESCE(enr.claimadjustedterminationdate, '9999-01-01') <= COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01')
                        THEN enr.claimadjustedterminationdate
        WHEN COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01') <= COALESCE(terminationdate, '9999-01-01') AND COALESCE(enr.rxclaimadjustedterminationdate, '9999-01-01') <= COALESCE(enr.terminationdate, '9999-01-01')
                        THEN enr.rxclaimadjustedterminationdate
        ELSE NULL
    END                                                                                  AS terminationdate,

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
FROM enr_adj enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid

WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(rxindicator, 'X') = '1'
