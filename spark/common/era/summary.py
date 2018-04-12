from pyspark.sql.types import StructType, StructField, LongType, FloatType, IntegerType, StringType, DateType

schema_v1 = StructType([
    StructField('row_id',                            LongType(),     True),
    StructField('hv_medcl_clm_pymt_sumry_id',        StringType(),   True),
    StructField('crt_dt',                            DateType(),     True),
    StructField('mdl_vrsn_num',                      StringType(),   True),
    StructField('data_set_nm',                       StringType(),   True),
    StructField('src_vrsn_id',                       StringType(),   True),
    StructField('hvm_vdr_id',                        IntegerType(),  True),
    StructField('hvm_vdr_feed_id',                   IntegerType(),  True),
    StructField('vdr_org_id',                        StringType(),   True),
    StructField('vdr_medcl_clm_pymt_sumry_id',       StringType(),   True),
    StructField('vdr_medcl_clm_pymt_sumry_id_qual',  StringType(),   True),
    StructField('hvid',                              StringType(),   True),
    StructField('ptnt_birth_yr',                     IntegerType(),  True),
    StructField('ptnt_age_num',                      StringType(),   True),
    StructField('ptnt_gender_cd',                    StringType(),   True),
    StructField('ptnt_state_cd',                     StringType(),   True),
    StructField('ptnt_zip3_cd',                      StringType(),   True),
    StructField('clm_stmt_perd_start_dt',            DateType(),     True),
    StructField('clm_stmt_perd_end_dt',              DateType(),     True),
    StructField('payr_clm_recpt_dt',                 DateType(),     True),
    StructField('payr_id',                           StringType(),   True),
    StructField('payr_id_qual',                      StringType(),   True),
    StructField('payr_nm',                           StringType(),   True),
    StructField('bllg_prov_npi',                     StringType(),   True),
    StructField('bllg_prov_vdr_id',                  StringType(),   True),
    StructField('bllg_prov_vdr_id_qual',             StringType(),   True),
    StructField('bllg_prov_tax_id',                  StringType(),   True),
    StructField('bllg_prov_ssn',                     StringType(),   True),
    StructField('bllg_prov_state_lic_id',            StringType(),   True),
    StructField('bllg_prov_upin',                    StringType(),   True),
    StructField('bllg_prov_comrcl_id',               StringType(),   True),
    StructField('bllg_prov_nabp_id',                 StringType(),   True),
    StructField('bllg_prov_1_nm',                    StringType(),   True),
    StructField('bllg_prov_2_nm',                    StringType(),   True),
    StructField('bllg_prov_addr_1_txt',              StringType(),   True),
    StructField('bllg_prov_addr_2_txt',              StringType(),   True),
    StructField('bllg_prov_city_nm',                 StringType(),   True),
    StructField('bllg_prov_state_cd',                StringType(),   True),
    StructField('bllg_prov_zip_cd',                  StringType(),   True),
    StructField('clm_prov_pymt_amt',                 FloatType(),    True),
    StructField('clm_prov_pymt_amt_qual',            StringType(),   True),
    StructField('clm_prov_pymt_dt',                  DateType(),     True),
    StructField('clm_stat_cd',                       StringType(),   True),
    StructField('clm_submtd_chg_amt',                FloatType(),    True),
    StructField('clm_pymt_amt',                      FloatType(),    True),
    StructField('ptnt_respbty_amt',                  FloatType(),    True),
    StructField('medcl_covrg_typ_cd',                StringType(),   True),
    StructField('ptnt_ctl_num',                      StringType(),   True),
    StructField('payr_clm_ctl_num',                  StringType(),   True),
    StructField('pos_cd',                            StringType(),   True),
    StructField('instnl_typ_of_bll_cd',              StringType(),   True),
    StructField('drg_cd',                            StringType(),   True),
    StructField('drg_weight_num',                    FloatType(),    True),
    StructField('dischg_frctn_num',                  FloatType(),    True),
    StructField('rndrg_prov_npi',                    StringType(),   True),
    StructField('rndrg_prov_vdr_id',                 StringType(),   True),
    StructField('rndrg_prov_tax_id',                 StringType(),   True),
    StructField('rndrg_prov_ssn',                    StringType(),   True),
    StructField('rndrg_prov_state_lic_id',           StringType(),   True),
    StructField('rndrg_prov_upin',                   StringType(),   True),
    StructField('rndrg_prov_comrcl_id',              StringType(),   True),
    StructField('rndrg_prov_1_nm',                   StringType(),   True),
    StructField('rndrg_prov_2_nm',                   StringType(),   True),
    StructField('rndrg_prov_addr_1_txt',             StringType(),   True),
    StructField('rndrg_prov_addr_2_txt',             StringType(),   True),
    StructField('rndrg_prov_city_nm',                StringType(),   True),
    StructField('rndrg_prov_state_cd',               StringType(),   True),
    StructField('rndrg_prov_zip_cd',                 StringType(),   True),
    StructField('cob_pyr_nm',                        StringType(),   True),
    StructField('cob_pyr_id',                        StringType(),   True),
    StructField('cob_pyr_id_qual',                   StringType(),   True),
    StructField('covrd_day_vst_cnt',                 IntegerType(),  True),
    StructField('pps_operg_outlr_amt',               FloatType(),    True),
    StructField('lftm_psychtrc_day_cnt',             IntegerType(),  True),
    StructField('clm_drg_amt',                       FloatType(),    True),
    StructField('clm_dsh_amt',                       FloatType(),    True),
    StructField('clm_msp_pass_thru_amt',             FloatType(),    True),
    StructField('clm_pps_captl_amt',                 FloatType(),    True),
    StructField('pps_captl_fsp_drg_amt',             FloatType(),    True),
    StructField('pps_captl_hsp_drg_amt',             FloatType(),    True),
    StructField('pps_captl_dsh_drg_amt',             FloatType(),    True),
    StructField('prev_rptg_perd_captl_amt',          FloatType(),    True),
    StructField('pps_captl_ime_amt',                 FloatType(),    True),
    StructField('pps_operg_hsp_drg_amt',             FloatType(),    True),
    StructField('cost_rpt_day_cnt',                  IntegerType(),  True),
    StructField('pps_operg_fsp_drg_amt',             FloatType(),    True),
    StructField('clm_pps_captl_outlr_amt',           FloatType(),    True),
    StructField('clm_indrct_tchg_amt',               FloatType(),    True),
    StructField('non_paybl_profnl_compnt_amt',       FloatType(),    True),
    StructField('clm_pymt_remrk_cd_seq_num',         IntegerType(),  True),
    StructField('clm_pymt_remrk_cd',                 StringType(),   True),
    StructField('pps_captl_excptn_amt',              FloatType(),    True),
    StructField('reimbmt_rate_pct',                  FloatType(),    True),
    StructField('clm_hcpcs_paybl_amt',               FloatType(),    True),
    StructField('clm_esrd_pymt_amt',                 FloatType(),    True),
    StructField('clm_amt_seq_num',                   IntegerType(),    True),
    StructField('clm_amt',                           FloatType(),    True),
    StructField('clm_amt_qual',                      StringType(),   True),
    StructField('clm_qty_seq_num',                   IntegerType(),  True),
    StructField('clm_qty',                           IntegerType(),  True),
    StructField('clm_qty_qual',                      StringType(),   True),
    StructField('clm_adjmt_seq_num',                 IntegerType(),  True),
    StructField('clm_adjmt_grp_cd',                  StringType(),   True),
    StructField('clm_adjmt_rsn_txt',                 StringType(),   True),
    StructField('clm_adjmt_amt',                     FloatType(),    True),
    StructField('clm_adjmt_qty',                     IntegerType(),  True),
    StructField('clm_fnctnl_grp_ctl_num',            StringType(),   True)
])