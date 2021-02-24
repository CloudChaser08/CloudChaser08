SELECT
    rxclaimuid       ,	
    memberuid	     ,
    provideruid	     ,
    claimstatuscode	 ,
    filldate         ,
    ndc11code        ,
    supplydayscount  ,
    dispensedquantity,
    billedamount     ,
    allowedamount	 ,
    copayamount      ,
    paidamount	     ,
    costamount       ,
    prescribingnpi   ,
    dispensingnpi    ,
    sourcemodifieddate,
    createddate       ,
    input_file_name
FROM
    (
        SELECT
        rxclaimuid       ,	
        memberuid	     ,
        provideruid	     ,
        claimstatuscode	 ,
        filldate         ,
        ndc11code        ,
        supplydayscount  ,
        dispensedquantity,
        billedamount     ,
        allowedamount	 ,
        copayamount      ,
        paidamount	     ,
        costamount       ,
        prescribingnpi   ,
        dispensingnpi    ,
        sourcemodifieddate,
        createddate       ,
        input_file_name   ,
           ROW_NUMBER() OVER (PARTITION BY 
                memberuid	     ,
                provideruid	     ,
                claimstatuscode	 , 
                filldate         ,
                ndc11code        ,
                supplydayscount  ,
                dispensedquantity,
                billedamount     ,
                allowedamount	 ,
                copayamount      ,
                paidamount	     ,
                costamount       ,
                prescribingnpi   ,
                dispensingnpi    ,
                createddate  
                ORDER BY 
                sourcemodifieddate DESC
                ) AS row_num    
        FROM rxc
        WHERE TRIM(lower(COALESCE(rxc.rxclaimuid, 'empty'))) <> 'rxclaimuid'
    )
WHERE row_num  = 1 
