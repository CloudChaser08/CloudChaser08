SELECT  
    hv_medcl_clm_pymt_sumry_id,
    crt_dt,
    mdl_vrsn_num,
    hvm_vdr_id,
    MAX(data_set_nm) AS data_set_nm,
    hvm_vdr_feed_id,
    vdr_medcl_clm_pymt_sumry_id,
    clm_typ_cd,
    vdr_medcl_clm_pymt_sumry_id_qual,
    clm_stmt_perd_start_dt,
    clm_stmt_perd_end_dt,
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
    clm_adjmt_seq_num,
    clm_adjmt_grp_cd,
    clm_adjmt_rsn_cd,
    clm_adjmt_amt,
    medcl_clm_lnk_txt,
    part_hvm_vdr_feed_id,
    part_mth
FROM 
practice_insight_era_summary
GROUP BY     
    hv_medcl_clm_pymt_sumry_id, crt_dt              , mdl_vrsn_num     , hvm_vdr_id                      ,    --MAX(data_set_nm) AS data_set_nm,    
    hvm_vdr_feed_id,    vdr_medcl_clm_pymt_sumry_id ,    clm_typ_cd    , vdr_medcl_clm_pymt_sumry_id_qual,
    clm_stmt_perd_start_dt    , clm_stmt_perd_end_dt, payr_clm_recpt_dt, payr_id               ,    payr_id_qual    ,    payr_nm                , bllg_prov_npi     , bllg_prov_tax_id    , bllg_prov_1_nm  , bllg_prov_addr_1_txt, bllg_prov_addr_2_txt,    bllg_prov_city_nm,
    bllg_prov_state_cd        , bllg_prov_zip_cd    , clm_prov_pymt_amt, clm_prov_pymt_amt_qual,    clm_prov_pymt_dt,    clm_stat_cd            , clm_submtd_chg_amt, clm_pymt_amt        , ptnt_respbty_amt, medcl_covrg_typ_cd  , payr_clm_ctl_num,
    pos_cd                    , instnl_typ_of_bll_cd, drg_cd           , drg_weight_num        ,    rndrg_prov_npi  ,    rndrg_prov_state_lic_id, rndrg_prov_upin   , rndrg_prov_comrcl_id, rndrg_prov_1_nm , clm_adjmt_seq_num   , clm_adjmt_grp_cd,
    clm_adjmt_rsn_cd          , clm_adjmt_amt       , medcl_clm_lnk_txt, part_hvm_vdr_feed_id  ,    part_mth