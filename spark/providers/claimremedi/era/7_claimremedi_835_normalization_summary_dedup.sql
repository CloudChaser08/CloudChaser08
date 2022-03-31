
  SELECT
    hv_medcl_clm_pymt_sumry_id,
    crt_dt,
    mdl_vrsn_num,
    hvm_vdr_id,
    data_set_nm,
    hvm_vdr_feed_id,
    vdr_medcl_clm_pymt_sumry_id,
    vdr_medcl_clm_pymt_sumry_id_qual,
    ------------------------------------------------------------------------------------------------
	--  if clm_stmt_perd_start_dt is NULL AND clm_stmt_perd_end_dt is NOT NULL , set it to clm_stmt_perd_end_dt.
    ------------------------------------------------------------------------------------------------
    CASE
        WHEN clm_stmt_perd_start_dt IS NULL AND clm_stmt_perd_end_dt IS NOT NULL THEN clm_stmt_perd_end_dt
    ELSE clm_stmt_perd_start_dt
    END                               AS clm_stmt_perd_start_dt,

    ------------------------------------------------------------------------------------------------
	--  if clm_stmt_perd_end_dt is NULL AND clm_stmt_perd_start_dt is NOT NULL , set it to clm_stmt_perd_start_dt.
    ------------------------------------------------------------------------------------------------
    CASE
        WHEN clm_stmt_perd_end_dt IS NULL      AND clm_stmt_perd_start_dt IS NOT NULL THEN clm_stmt_perd_start_dt
    ------------------------------------------------------------------------------------------------
	--  if clm_stmt_perd_end_dt is NOT NULL AND clm_stmt_perd_start_dt is NOT NULL , AND clm_stmt_perd_end_dt < clm_stmt_perd_start_dt set it to clm_stmt_perd_start_dt.
    ------------------------------------------------------------------------------------------------
        WHEN clm_stmt_perd_end_dt IS NOT NULL AND clm_stmt_perd_start_dt IS NOT NULL
            AND clm_stmt_perd_end_dt < clm_stmt_perd_start_dt                    THEN clm_stmt_perd_start_dt
    ELSE clm_stmt_perd_end_dt
    END                               AS clm_stmt_perd_end_dt,
    ------------------------------------------------------------------------------------------------
    payr_clm_recpt_dt,
    payr_id,
    payr_id_qual,
    payr_nm,
    bllg_prov_npi,
    bllg_prov_tax_id,
    bllg_prov_1_nm,
    bllg_prov_addr_1_txt,
    bllg_prov_addr_2_txt,
    bllg_prov_city_nm,
    bllg_prov_state_cd,
    bllg_prov_zip_cd,
    clm_prov_pymt_amt,
    clm_prov_pymt_amt_qual,
    clm_prov_pymt_dt,
    clm_stat_cd,
    clm_submtd_chg_amt,
    clm_pymt_amt,
    ptnt_respbty_amt,
    medcl_covrg_typ_cd,
    payr_clm_ctl_num,
    pos_cd,
    instnl_typ_of_bll_cd,
    drg_cd,
    drg_weight_num,
    rndrg_prov_npi,
    rndrg_prov_state_lic_id,
    rndrg_prov_upin,
    rndrg_prov_comrcl_id,
    rndrg_prov_1_nm,
    rndrg_prov_2_nm,
    clm_adjmt_seq_num,
    clm_adjmt_grp_cd,
    clm_adjmt_rsn_cd,
    clm_adjmt_amt,
    medcl_clm_lnk_txt,
    clm_typ_cd,
    part_hvm_vdr_feed_id,
    part_mth
FROM
    claimremedi_835_normalization_summary
GROUP BY
  1,   2, 	3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,
 21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,
 41,  42,  43,  44,  45,  46,  47,  48, 49

--limit 1
