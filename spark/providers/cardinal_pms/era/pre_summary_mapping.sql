SELECT
    *,
    claim.id                                    AS the_claim_id,
    densify_scalar_array(ARRAY(
        claim.claimpaymentremarkcode,
        claim.claimpaymentremarkcodeone,
        claim.claimpaymentremarkcodetwo,
        claim.claimpaymentremarkcodethree,
        claim.claimpaymentremarkcodefour
    ))                                          AS dense_clm_pymt_remrk_cd,
    densify_2d_array(ARRAY(
        ARRAY(claim.coverageamount, COALESCE(claim.coverageamountqualifiercode, 'AU')),
        ARRAY(claim.discountamount, COALESCE(claim.discountamountqualifiercode, 'D8')),
        ARRAY(claim.fedmedcat1amount, COALESCE(claim.fedmedcat1amountqualifiercode, 'ZK')),
        ARRAY(claim.fedmedcat2amount, COALESCE(claim.fedmedcat2amountqualifiercode, 'ZL')),
        ARRAY(claim.fedmedcat3amount, COALESCE(claim.fedmedcat3amountqualifiercode, 'ZM')),
        ARRAY(claim.fedmedcat4amount, COALESCE(claim.fedmedcat4amountqualifiercode, 'ZN')),
        ARRAY(claim.fedmedcat5amount, COALESCE(claim.fedmedcat5amountqualifiercode, 'ZO')),
        ARRAY(claim.interestamount, COALESCE(claim.interestamountqualifiercode, 'I')),
        ARRAY(claim.negativeledgeramount, COALESCE(claim.negativeledgeramountqualifiercode, 'NL')),
        ARRAY(claim.nonpayableprofessionalcomponentamount, NULL),
        ARRAY(claim.perdiemamount, COALESCE(claim.perdiemamountqualifiercode, 'DY')),
        ARRAY(claim.totalclaimbeforetaxesamount, COALESCE(claim.totalclaimbeforetaxesamountqualifiercode, 'T2')),
        ARRAY(claim.patientpaidamount, COALESCE(claim.patientpaidamountqualifiercode, 'F5'))
    ))                                          AS dense_clm_amt,
    densify_2d_array(ARRAY(
        ARRAY(claim.coinsuredquantity, claim.coinsuredqualifiercode),
        ARRAY(claim.fedmedcatonequantity, claim.fedmedcatonequantityqualifiercode),
        ARRAY(claim.fedmedcattwoquantity, claim.fedmedcattwoquantityqualifiercode),
        ARRAY(claim.fedmedcatthreequantity, claim.fedmedcatthreequantityqualifiercode),
        ARRAY(claim.fedmedcatfourquantity, claim.fedmedcatfourquantityqualifiercode),
        ARRAY(claim.fedmedcatfivequantity, claim.fedmedcatfivequantityqualifiercode),
        ARRAY(claim.lifetimereservedactualquantity, claim.lifetimereservedactualqualifiercode),
        ARRAY(claim.lifetimereservedestimatedquantity, claim.lifetimereservedestimatedqualifiercode),
        ARRAY(claim.noncoveredquantity, claim.noncoveredqualifiercode),
        ARRAY(claim.notreplacedquantity, claim.notreplacedqualifiercode),
        ARRAY(claim.outlierdaysquantity, claim.outlierdaysqualifiercode),
        ARRAY(claim.prescriptionquantity, claim.prescriptionqualifiercode),
        ARRAY(claim.visitsquantity, claim.visitsqualifiercode)
    ))                                          AS dense_clm_qty,
    densify_2d_array(ARRAY(
        ARRAY(adjust.adjustmentreasoncodeone, adjust.adjustmentamountone, adjust.adjustmentquantityone),
        ARRAY(adjust.adjustmentreasoncodetwo, adjust.adjustmentamounttwo, adjust.adjustmentquantitytwo),
        ARRAY(adjust.adjustmentreasoncodethree, adjust.adjustmentamountthree, adjust.adjustmentquantitythree),
        ARRAY(adjust.adjustmentreasoncodefour, adjust.adjustmentamountfour, adjust.adjustmentquantityfour),
        ARRAY(adjust.adjustmentreasoncodefive, adjust.adjustmentamountfive, adjust.adjustmentquantityfive),
        ARRAY(adjust.adjustmentreasoncodesix, adjust.adjustmentamountsix, adjust.adjustmentquantitysix)
    ))                                          AS dense_clm_adjmt
FROM
    remit_claim claim
    LEFT JOIN
        remit_claim_adjustment adjust
            ON claim.id = adjust.remitclaim_id
    LEFT JOIN
        serviceline service
            ON claim.id = service.remitclaim_id
