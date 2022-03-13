SELECT
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
  clm.claimstatuscode,
  -------- Amount
  clm.billedamount,
  clm.allowedamount,
  clm.copayamount,
  clm.paidamount,
  clm.costamount,
  clm.serviceunitquantity,
  clm.renderingprovidernpi,
  clm.renderingprovideruid,
  clm.billingprovidernpi,
  clm.billingprovideruid,
  ----- tobpos
  tob_pos.pos_code,
  tob_pos.tob_code,
--  pivot.proc_code,
--  pivot.proc_mod,
--  pivot.diag_code,
--  pivot.diag_qualifier,
--  pivot.admit_diag_code_fld,
--  pivot.apdrg,
--  pivot.msdrg,
--  pivot.rev_code,
  clm.input_file_name--,
--  'end'
FROM
  inv_norm_0_dx_transaction clm
--  LEFT OUTER JOIN inv_norm_10_dx_pivot pivot ON clm.claimuid = pivot.claimuid --------------------- JOIN the PIVOT Table
  ----- OUTER JOIN with TOB/POS
  LEFT OUTER JOIN inv_norm_20_dx_tobpos tob_pos ON clm.claimuid = tob_pos.claimuid --------------------- JOIN the POS/TOB Table
  ----------------------------
ORDER BY
  clm.claimuid