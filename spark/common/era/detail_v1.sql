DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        row_id                              bigint,
        hv_medcl_clm_pymt_dtl_id            string,
        crt_dt                              date,
        mdl_vrsn_num                        string,
        data_set_nm                         string,
        src_vrsn_id                         string,
        hvm_vdr_id                          int,
        hvm_vdr_feed_id                     int,
        vdr_org_id                          string,
        vdr_medcl_clm_pymt_sumry_id         string,
        vdr_medcl_clm_pymt_sumry_id_qual    string,
        vdr_medcl_clm_pymt_dtl_id           string,
        vdr_medcl_clm_pymt_dtl_id_qual      string,
        hvid                                string,
        ptnt_birth_yr                       int,
        ptnt_age_num                        string,
        ptnt_gender_cd                      string,
        ptnt_state_cd                       string,
        ptnt_zip3_cd                        string,
        svc_ln_start_dt                     date,
        svc_ln_end_dt                       date,
        rndrg_prov_npi                      string,
        rndrg_prov_vdr_id                   string,
        rndrg_prov_tax_id                   string,
        rndrg_prov_ssn                      string,
        rndrg_prov_state_lic_id             string,
        rndrg_prov_upin                     string,
        rndrg_prov_comrcl_id                string,
        adjctd_proc_cd                      string,
        adjctd_proc_cd_qual                 string,
        adjctd_proc_cd_1_modfr              string,
        adjctd_proc_cd_2_modfr              string,
        adjctd_proc_cd_3_modfr              string,
        adjctd_proc_cd_4_modfr              string,
        orig_submtd_adjctd_proc_cd          string,
        orig_submtd_adjctd_proc_cd_qual     string,
        orig_submtd_adjctd_proc_cd_1_modfr  string,
        orig_submtd_adjctd_proc_cd_2_modfr  string,
        orig_submtd_adjctd_proc_cd_3_modfr  string,
        orig_submtd_adjctd_proc_cd_4_modfr  string,
        svc_ln_submtd_chg_amt               float,
        svc_ln_prov_pymt_amt                float,
        rev_cd                              string,
        ndc_cd                              string,
        paid_svc_unt_cnt                    int,
        orig_svc_unt_cnt                    int,
        svc_ln_adjmt_grp_cd                 string,
        svc_ln_adjmt_seq_num                int,
        svc_ln_adjmt_rsn_txt                string,
        svc_ln_adjmt_amt                    float,
        svc_ln_adjmt_qty                    int,
        svc_ln_prov_ctl_num                 string,
        svc_ln_suplmtl_amt                  float,
        svc_ln_suplmtl_amt_qual             string,
        svc_ln_rmrk_cd_seq_num              int,
        svc_ln_remrk_cd                     string,
        svc_ln_remrk_cd_qual                string
    )
{properties}
;
