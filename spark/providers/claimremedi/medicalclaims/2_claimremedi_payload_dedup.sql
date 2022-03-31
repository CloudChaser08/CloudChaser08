SELECT
    hvid            ,
--    lastName        ,
--    firstName       ,
    personId        ,
    claimId         ,
    yearOfBirth     ,
    threeDigitZip   ,
    gender          ,
    age             ,
    state           ,
    hvJoinKey
FROM matching_payload
GROUP BY
    hvid            ,
--    lastName        ,
--    firstName       ,
    personId        ,
    claimId         ,
    yearOfBirth     ,
    threeDigitZip   ,
    gender          ,
    age             ,
    state           ,
    hvJoinKey