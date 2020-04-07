SELECT
    clm.hv_enc_id,
    clm.hvid,
    clm.gender,
    clm.threedigitzip,
    clm.age,
    clm.yearofbirth,
    clm.state,
    clm.date_service,
    clm.date_service_end,
    clm.claim_type,
    clm.claimuid, 
    clm.memberuid,
    clm.createddate, 
    clm.servicedate, 
    clm.servicethrudate,
    clm.provideruid,
    ------------ Claim Type
    clm.claimformtypecode,
    clm.institutionaltypecode,
    clm.professionaltypecode, 
    clm.ubpatientdischargestatuscode,
    ------------ Claim status
    clm.claimstatuscode,
    -------- Amount
    clm.billedamount,
    clm.allowedamount,
    clm.copayamount,
    clm.paidamount,
    clm.costamount, 
    -------- Quantity
    clm.serviceunitquantity,
    --- Provider infor rendering
    clm.renderingprovidernpi,
    clm.renderingprovideruid,
    -- billing
    clm.billingprovidernpi,
    clm.billingprovideruid, 

    pivot.pos_code,
    pivot.tob_code,  
    ------------ procedure
    pivot.proc_code,
    ------------ procedure modifier
    pivot.proc_mod,
    ------------  diag code      
    pivot.diag_code,
    ------------ diag qualifier 
    pivot.diag_qualifier,
    ------------ admin code flag
    pivot.admit_diag_code_fld,
    ------------ apdrg, msdrg, revcode
    pivot.apdrg, 
    pivot.msdrg, 
    pivot.rev_code,  
    clm.input_file_name,
	'end'	
FROM inovalon_01_claims_with_demo clm
--------------------- JOIN the PIVOT Table
LEFT OUTER JOIN inovalon_20_dx_pivot pivot
    ON clm.claimuid = pivot.claimuid
