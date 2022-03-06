SELECT
    base_prep.*,

    pivot.proc_code,
    pivot.proc_mod,
    pivot.diag_code,
    pivot.diag_qualifier,
    pivot.admit_diag_code_fld,
    pivot.apdrg,
    pivot.msdrg,
    pivot.rev_code,
	'end'
FROM inv_norm_30_dx_base_prep base_prep
LEFT OUTER JOIN inv_norm_10_dx_pivot     pivot  ON base_prep.claimuid    = pivot.claimuid    --------------------- JOIN the PIVOT Table
----- OUTER JOIN with TOB/POS
--LEFT OUTER JOIN inv_norm_20_dx_tobpos tob_pos   ON clm.claimuid    = tob_pos.claimuid  --------------------- JOIN the POS/TOB Table


----------------------------
ORDER BY base_prep.claimuid