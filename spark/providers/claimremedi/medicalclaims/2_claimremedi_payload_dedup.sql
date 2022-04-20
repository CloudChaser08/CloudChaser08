 SELECT
     hvid            ,
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
     personId        ,
     claimId         ,
     yearOfBirth     ,
     threeDigitZip   ,
     gender          ,
     age             ,
     state           ,
     hvJoinKey
