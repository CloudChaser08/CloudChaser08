SELECT clm.*,
CONCAT
(
    '543','_',
    COALESCE(pay.hvid, clm.memberuid),
    COALESCE(CONCAT('_', CAST(clm.servicedate AS DATE),'')),
    COALESCE(CONCAT('_', clm.provideruid),'')
)                                                                                       AS hv_enc_id,
   pay.hvid,
CASE
    WHEN SUBSTR(UPPER(mbr.gendercode), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(mbr.gendercode), 1, 1)
    WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
END                                                                                 AS gender,
COALESCE(mbr.birthyear, pay.yearofbirth)                                            AS yearofbirth,
pay.age,
CASE
    WHEN 3 = LENGTH(TRIM(COALESCE(mbr.zip3value, ''))) THEN mbr.zip3value
    ELSE pay.threedigitzip
END                                                                                 AS threedigitzip,
COALESCE(mbr.statecode, pay.state, '')                                              AS state,
CAP_DATE
(
    CAST(clm.servicedate AS DATE),
    CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
    CAST('{VDR_FILE_DT}' AS DATE)
)                                                                                   AS date_service,
CAP_DATE
(
    CAST(clm.servicethrudate AS DATE),
    CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
    CAST('{VDR_FILE_DT}' AS DATE)
)                                                                                   AS date_service_end,
    CASE
        WHEN UPPER(clm.claimformtypecode)     IN ('I','P')     THEN UPPER(claimformtypecode)
        WHEN UPPER(clm.institutionaltypecode) IN ('I','O','U') THEN 'I'
        WHEN UPPER(clm.professionaltypecode)  IN ('I','O','U') then 'P'
        ELSE NULL
    END                                                                                     AS claim_type
FROM clm
LEFT OUTER JOIN matching_payload pay       ON clm.memberuid    = pay.claimid
LEFT OUTER JOIN mbr                        ON clm.memberuid    = mbr.memberuid
