SELECT DISTINCT
    enr.memberuid,
    enr.effectivedate                                                                   AS orig_effectivedate,
    enr.terminationdate                                                                 AS orig_terminationdate,
    --------------------------------------------------------------------------------------------------------------
    -----------------------------------New Logic introduced (JKS - 2021-08-17)------------------------------------
    --------------------------------------------------------------------------------------------------------------
    CASE
        WHEN enr.effectivedate >= enr.claimadjustedeffectivedate   AND enr.effectivedate                >= enr.rxclaimadjustedeffectivedate THEN enr.effectivedate
        WHEN enr.claimadjustedeffectivedate >= enr.effectivedate   AND enr.claimadjustedeffectivedate   >= enr.rxclaimadjustedeffectivedate THEN enr.claimadjustedeffectivedate
        WHEN enr.rxclaimadjustedeffectivedate >= enr.effectivedate AND enr.rxclaimadjustedeffectivedate >= enr.claimadjustedeffectivedate   THEN enr.rxclaimadjustedeffectivedate
        ELSE NULL
    END                                                                                  AS effectivedate,

    CASE
        WHEN enr.terminationdate <= claimadjustedterminationdate   AND enr.terminationdate                <= enr.rxclaimadjustedterminationdate THEN enr.terminationdate
        WHEN enr.claimadjustedterminationdate <= terminationdate   AND enr.claimadjustedterminationdate   <= enr.rxclaimadjustedterminationdate THEN enr.claimadjustedterminationdate
        WHEN enr.rxclaimadjustedterminationdate <= terminationdate AND enr.rxclaimadjustedterminationdate <= enr.terminationdate                THEN enr.rxclaimadjustedterminationdate
        ELSE NULL
    END                                                                                  AS terminationdate,

    /* effectivedate Below codes are removed after discussion with Will 2021-08-17*/
    -- CASE
    --     WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)  THEN NULL
    --     WHEN enr.effectivedate < '{EARLIEST_SERVICE_DATE}' THEN NULL
    --     ELSE enr.effectivedate
    -- END                                                                                 AS effectivedate,
    -- /* terminationdate */
    -- CASE
    --     WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)                THEN NULL
    --     WHEN enr.effectivedate < '{EARLIEST_SERVICE_DATE}'               THEN NULL
    --     WHEN enr.terminationdate > CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31') THEN CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
    --     ELSE enr.terminationdate
    -- END                                                                                 AS terminationdate,
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
 FROM enr_adj as enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
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
        WHEN enr.effectivedate >= enr.claimadjustedeffectivedate   AND enr.effectivedate                >= enr.rxclaimadjustedeffectivedate THEN enr.effectivedate
        WHEN enr.claimadjustedeffectivedate >= enr.effectivedate   AND enr.claimadjustedeffectivedate   >= enr.rxclaimadjustedeffectivedate THEN enr.claimadjustedeffectivedate
        WHEN enr.rxclaimadjustedeffectivedate >= enr.effectivedate AND enr.rxclaimadjustedeffectivedate >= enr.claimadjustedeffectivedate   THEN enr.rxclaimadjustedeffectivedate
        ELSE NULL
    END                                                                                  AS effectivedate,
    -------------------Earliest date is the terminate date -------------------------------------------------------
    CASE
        WHEN enr.terminationdate <= claimadjustedterminationdate   AND enr.terminationdate                <= enr.rxclaimadjustedterminationdate THEN enr.terminationdate
        WHEN enr.claimadjustedterminationdate <= terminationdate   AND enr.claimadjustedterminationdate   <= enr.rxclaimadjustedterminationdate THEN enr.claimadjustedterminationdate
        WHEN enr.rxclaimadjustedterminationdate <= terminationdate AND enr.rxclaimadjustedterminationdate <= enr.terminationdate                THEN enr.rxclaimadjustedterminationdate
        ELSE NULL
    END                                                                                  AS terminationdate,

    /* effectivedate Below codes are removed after discussion with Will 2021-08-17*/
    -- CASE
    --     WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)  THEN NULL
    --     WHEN enr.effectivedate < '{EARLIEST_SERVICE_DATE}' THEN NULL
    --     ELSE enr.effectivedate
    -- END                                                                                 AS effectivedate,
    -- /* terminationdate */
    -- CASE
    --     WHEN enr.effectivedate > DATE_ADD('{VDR_FILE_DT}', 365)  THEN NULL
    --     WHEN enr.effectivedate <'{EARLIEST_SERVICE_DATE}'  THEN NULL
    --     WHEN enr.terminationdate > CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31') THEN CONCAT(1 + YEAR('{VDR_FILE_DT}'), '-12-31')
    --     ELSE enr.terminationdate
    -- END                                                                                 AS terminationdate,
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
FROM enr_adj as enr
LEFT OUTER JOIN mbr ON enr.memberuid = mbr.memberuid
LEFT OUTER JOIN matching_payload pay ON enr.memberuid = pay.claimid

WHERE UPPER(COALESCE(enr.memberuid, 'X')) <> 'MEMBERUID'
  AND COALESCE(rxindicator, 'X') = '1'
