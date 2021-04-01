SELECT clm.*,
CONCAT
(
    '543','_',
    COALESCE(pay.hvid, clm.memberuid),
    COALESCE(CONCAT('_', CAST(EXTRACT_DATE(clm.servicedate, '%Y-%m-%d') AS DATE)),''),
    COALESCE(CONCAT('_', clm.provideruid),'')
)                                                                                       AS hv_enc_id,
COALESCE(pay.hvid, CONCAT('543_', clm.memberuid))                                       AS hvid,
CASE
    WHEN SUBSTR(UPPER(mbr.gendercode), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(mbr.gendercode), 1, 1)
    WHEN SUBSTR(UPPER(pay.gender)    , 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender)    , 1, 1)
    ELSE 'U'
END                                                                                     AS gender,
CAP_AGE
    (
    VALIDATE_AGE
        (
            pay.age,
            CAST(EXTRACT_DATE(clm.servicedate, '%Y-%m-%d') AS DATE),
            COALESCE(mbr.birthyear, pay.yearofbirth)
        )
    )                                                                                   AS age,
CAP_YEAR_OF_BIRTH
    (
        pay.age,
        CAST(EXTRACT_DATE(clm.servicedate, '%Y-%m-%d') AS DATE),
        COALESCE(mbr.birthyear,pay.yearofbirth)
    )                                                                                   AS yearofbirth,
MASK_ZIP_CODE
    (
        CASE
            WHEN 3 = LENGTH(TRIM(COALESCE(mbr.zip3value, ''))) THEN mbr.zip3value
            ELSE pay.threedigitzip
        END
    )                                                                                   AS threedigitzip,
VALIDATE_STATE_CODE(UPPER(COALESCE(mbr.statecode, pay.state, '')))                      AS state,
CASE
    WHEN UPPER(clm.claimformtypecode)     IN ('I','P')     THEN UPPER(claimformtypecode)
    WHEN UPPER(clm.institutionaltypecode) IN ('I','O','U') THEN 'I'
    WHEN UPPER(clm.professionaltypecode)  IN ('I','O','U') then 'P'
    ELSE NULL
END                                                                                     AS claim_type,
CAP_DATE
    (
        CAST(EXTRACT_DATE(clm.createddate               , '%Y-%m-%d') AS DATE),
        CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS date_received,
CAP_DATE
    (
        CAST(EXTRACT_DATE(clm.servicedate               , '%Y-%m-%d') AS DATE),
        CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS date_service,
CAP_DATE
    (
        CAST(EXTRACT_DATE(clm.servicethrudate           , '%Y-%m-%d') AS DATE),
        CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS date_service_end
FROM clm
LEFT OUTER JOIN matching_payload pay       ON clm.memberuid    = pay.claimid
LEFT OUTER JOIN mbr                        ON clm.memberuid    = mbr.memberuid
