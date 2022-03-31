
 SELECT
    row_id,
    hv_medcl_clm_pymt_dtl_id,
    crt_dt,
    mdl_vrsn_num,
    hvm_vdr_id,
    data_set_nm,
    hvm_vdr_feed_id,
    vdr_medcl_clm_pymt_sumry_id,
    vdr_medcl_clm_pymt_sumry_id_qual,
    vdr_medcl_clm_pymt_dtl_id,
    vdr_medcl_clm_pymt_dtl_id_qual,
    ------------------------------------------------------------------------------------------------
	--  if svc_ln_start_dt is NULL AND svc_ln_end_dt is NOT NULL , set it to svc_ln_end_dt.
    ------------------------------------------------------------------------------------------------
    CASE
        WHEN svc_ln_start_dt IS NULL AND svc_ln_end_dt IS NOT NULL THEN svc_ln_end_dt
        ELSE svc_ln_start_dt
    END                                AS svc_ln_start_dt,
    ------------------------------------------------------------------------------------------------
	--  if svc_ln_end_dt is NULL AND svc_ln_start_dt is NOT NULL , set it to svc_ln_start_dt.
    ------------------------------------------------------------------------------------------------
    CASE
        WHEN svc_ln_end_dt IS NULL AND svc_ln_start_dt IS NOT NULL THEN svc_ln_start_dt
        ELSE svc_ln_end_dt
    END                               AS svc_ln_end_dt,
    rndrg_prov_npi,
    rndrg_prov_state_lic_id,
    rndrg_prov_upin,
    rndrg_prov_comrcl_id,
    adjctd_proc_cd,
    adjctd_proc_cd_qual,
    adjctd_proc_cd_1_modfr,
    adjctd_proc_cd_2_modfr,
    adjctd_proc_cd_3_modfr,
    adjctd_proc_cd_4_modfr,
    orig_submtd_proc_cd,
    orig_submtd_proc_cd_qual,
    orig_submtd_proc_cd_1_modfr,
    orig_submtd_proc_cd_2_modfr,
    orig_submtd_proc_cd_3_modfr,
    orig_submtd_proc_cd_4_modfr,
    svc_ln_submtd_chg_amt,
    svc_ln_prov_pymt_amt,
    rev_cd,
    paid_svc_unt_cnt,
    orig_svc_unt_cnt,
    svc_ln_adjmt_grp_cd,
    svc_ln_adjmt_seq_num,
    svc_ln_adjmt_rsn_cd,
    svc_ln_adjmt_amt,
    svc_ln_vdr_ctl_num,
    svc_ln_suplmtl_amt,
    svc_ln_suplmtl_amt_qual,
    part_hvm_vdr_feed_id,
    part_mth
FROM claimremedi_835_normalization_detail
--limit 10
