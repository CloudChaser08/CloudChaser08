SELECT
    clm.hv_enc_id,
    clm.hvid,
    clm.gender,
    clm.age,
    clm.yearofbirth,
    clm.threedigitzip,
    clm.state,
    clm.claim_type,
    clm.date_received,
    clm.date_service,
    clm.date_service_end,
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

    tob_pos.pos_code,
    tob_pos.tob_code,
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
    pivot.input_file_name,
	'end'
FROM inovalon_01_claims_with_demo clm
LEFT OUTER JOIN inovalon_20_dx_pivot pivot            ON clm.claimuid    = pivot.claimuid    --------------------- JOIN the PIVOT Table
LEFT OUTER JOIN inovalon_claim_code_tob_pos tob_pos   ON clm.claimuid    = tob_pos.claimuid  --------------------- JOIN the POS/TOB Table
----------------------------
--ORDER BY clm.claimuid
--LIMIT 100