from pyspark.sql.types import StructType, StructField, LongType, FloatType, IntegerType, StringType, DateType

schema = StructType([
    StructField('row_id',                              LongType(),     True),
    StructField('hv_medcl_clm_pymt_dtl_id',            StringType(),   True),
    StructField('crt_dt',                              DateType(),     True),
    StructField('mdl_vrsn_num',                        StringType(),   True),
    StructField('data_set_nm',                         StringType(),   True),
    StructField('src_vrsn_id',                         StringType(),   True),
    StructField('hvm_vdr_id',                          IntegerType(),  True),
    StructField('hvm_vdr_feed_id',                     IntegerType(),  True),
    StructField('vdr_org_id',                          StringType(),   True),
    StructField('vdr_medcl_clm_pymt_sumry_id',         StringType(),   True),
    StructField('vdr_medcl_clm_pymt_sumry_id_qual',    StringType(),   True),
    StructField('vdr_medcl_clm_pymt_dtl_id',           StringType(),   True),
    StructField('vdr_medcl_clm_pymt_dtl_id_qual',      StringType(),   True),
    StructField('hvid',                                StringType(),   True),
    StructField('ptnt_birth_yr',                       IntegerType(),  True),
    StructField('ptnt_age_num',                        StringType(),   True),
    StructField('ptnt_gender_cd',                      StringType(),   True),
    StructField('ptnt_state_cd',                       StringType(),   True),
    StructField('ptnt_zip3_cd',                        StringType(),   True),
    StructField('svc_ln_start_dt',                     DateType(),     True),
    StructField('svc_ln_end_dt',                       DateType(),     True),
    StructField('rndrg_prov_npi',                      StringType(),   True),
    StructField('rndrg_prov_vdr_id',                   StringType(),   True),
    StructField('rndrg_prov_tax_id',                   StringType(),   True),
    StructField('rndrg_prov_ssn',                      StringType(),   True),
    StructField('rndrg_prov_state_lic_id',             StringType(),   True),
    StructField('rndrg_prov_upin',                     StringType(),   True),
    StructField('rndrg_prov_comrcl_id',                StringType(),   True),
    StructField('adjctd_proc_cd',                      StringType(),   True),
    StructField('adjctd_proc_cd_qual',                 StringType(),   True),
    StructField('adjctd_proc_cd_1_modfr',              StringType(),   True),
    StructField('adjctd_proc_cd_2_modfr',              StringType(),   True),
    StructField('adjctd_proc_cd_3_modfr',              StringType(),   True),
    StructField('adjctd_proc_cd_4_modfr',              StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd',          StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd_qual',     StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd_1_modfr',  StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd_2_modfr',  StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd_3_modfr',  StringType(),   True),
    StructField('orig_submtd_adjctd_proc_cd_4_modfr',  StringType(),   True),
    StructField('svc_ln_submtd_chg_amt',               FloatType(),    True),
    StructField('svc_ln_prov_pymt_amt',                FloatType(),    True),
    StructField('rev_cd',                              StringType(),   True),
    StructField('ndc_cd',                              StringType(),   True),
    StructField('paid_svc_unt_cnt',                    IntegerType(),  True),
    StructField('orig_svc_unt_cnt',                    IntegerType(),  True),
    StructField('svc_ln_adjmt_grp_cd',                 StringType(),   True),
    StructField('svc_ln_adjmt_seq_num',                IntegerType(),  True),
    StructField('svc_ln_adjmt_rsn_txt',                StringType(),   True),
    StructField('svc_ln_adjmt_amt',                    FloatType(),    True),
    StructField('svc_ln_adjmt_qty',                    IntegerType(),  True),
    StructField('svc_ln_prov_ctl_num',                 StringType(),   True),
    StructField('svc_ln_suplmtl_amt',                  FloatType(),    True),
    StructField('svc_ln_suplmtl_amt_qual',             StringType(),   True),
    StructField('svc_ln_rmrk_cd_seq_num',              IntegerType(),  True),
    StructField('svc_ln_remrk_cd',                     StringType(),   True),
    StructField('svc_ln_remrk_cd_qual',                StringType(),   True)
])
