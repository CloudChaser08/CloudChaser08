SELECT
    row_id,
    hv_medcl_clm_pymt_dtl_id,
    crt_dt,
    mdl_vrsn_num,
    data_set_nm,
    hvm_vdr_id,
    hvm_vdr_feed_id,
    vdr_medcl_clm_pymt_sumry_id,
    vdr_medcl_clm_pymt_sumry_id_qual,
    vdr_medcl_clm_pymt_dtl_id,
    vdr_medcl_clm_pymt_dtl_id_qual,
    svc_ln_start_dt,
    svc_ln_end_dt,
    rndrg_prov_npi,
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
    ndc_cd,
    paid_svc_unt_cnt,
    orig_svc_unt_cnt,
    svc_ln_adjmt_grp_cd,
    svc_ln_adjmt_seq_num,
    svc_ln_adjmt_rsn_cd,
    svc_ln_adjmt_amt,
    svc_ln_adjmt_qty,
    svc_ln_suplmtl_amt,
    svc_ln_suplmtl_amt_qual,
    part_hvm_vdr_feed_id,
    ----------- Reset the part_mth per svc_ln_end_dt - service end date
    CASE
	    WHEN 
	    (
	    svc_ln_end_dt < CAST(${AVAILABLE_HISTORY_START_DATE} AS DATE)
		OR   svc_ln_end_dt > CAST('{VDR_FILE_DT}'              AS DATE) 
		) THEN '0_PREDATES_HVM_HISTORY'
	ELSE
	CONCAT
	(
	    SUBSTR(svc_ln_end_dt, 1, 4), '-',
	    SUBSTR(svc_ln_end_dt, 6, 2), '-01'
	)
	END                                                                                     AS part_mth
 FROM  change_835_normalized_detail_pre_final
