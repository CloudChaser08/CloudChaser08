DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        row_id                                bigint,
        hv_diag_id                            string,
        crt_dt                                date,
        mdl_vrsn_num                          string,
        data_set_nm                           string,
        src_vrsn_id                           string,
        hvm_vdr_id                            int,
        hvm_vdr_feed_id                       int,
        vdr_org_id                            string,
        vdr_diag_id                           string,
        vdr_diag_id_qual                      string,
        hvid                                  string,
        ptnt_birth_yr                         int,
        ptnt_age_num                          string,
        ptnt_lvg_flg                          string,
        ptnt_dth_dt                           date,
        ptnt_gender_cd                        string,
        ptnt_state_cd                         string,
        ptnt_zip3_cd                          string,
        hv_enc_id                             string,
        enc_dt                                date,
        diag_dt                               date,
        diag_rndrg_fclty_npi                  string,
        diag_rndrg_fclty_vdr_id               string,
        diag_rndrg_fclty_vdr_id_qual          string,
        diag_rndrg_fclty_alt_id               string,
        diag_rndrg_fclty_alt_id_qual          string,
        diag_rndrg_fclty_tax_id               string,
        diag_rndrg_fclty_dea_id               string,
        diag_rndrg_fclty_state_lic_id         string,
        diag_rndrg_fclty_comrcl_id            string,
        diag_rndrg_fclty_nucc_taxnmy_cd       string,
        diag_rndrg_fclty_alt_taxnmy_id        string,
        diag_rndrg_fclty_alt_taxnmy_id_qual   string,
        diag_rndrg_fclty_mdcr_speclty_cd      string,
        diag_rndrg_fclty_alt_speclty_id       string,
        diag_rndrg_fclty_alt_speclty_id_qual  string,
        diag_rndrg_fclty_fclty_nm             string,
        diag_rndrg_fclty_addr_1_txt           string,
        diag_rndrg_fclty_addr_2_txt           string,
        diag_rndrg_fclty_state_cd             string,
        diag_rndrg_fclty_zip_cd               string,
        diag_rndrg_prov_npi                   string,
        diag_rndrg_prov_vdr_id                string,
        diag_rndrg_prov_vdr_id_qual           string,
        diag_rndrg_prov_alt_id                string,
        diag_rndrg_prov_alt_id_qual           string,
        diag_rndrg_prov_tax_id                string,
        diag_rndrg_prov_dea_id                string,
        diag_rndrg_prov_state_lic_id          string,
        diag_rndrg_prov_comrcl_id             string,
        diag_rndrg_prov_upin                  string,
        diag_rndrg_prov_ssn                   string,
        diag_rndrg_prov_nucc_taxnmy_cd        string,
        diag_rndrg_prov_alt_taxnmy_id         string,
        diag_rndrg_prov_alt_taxnmy_id_qual    string,
        diag_rndrg_prov_mdcr_speclty_cd       string,
        diag_rndrg_prov_alt_speclty_id        string,
        diag_rndrg_prov_alt_speclty_id_qual   string,
        diag_rndrg_prov_frst_nm               string,
        diag_rndrg_prov_last_nm               string,
        diag_rndrg_prov_addr_1_txt            string,
        diag_rndrg_prov_addr_2_txt            string,
        diag_rndrg_prov_state_cd              string,
        diag_rndrg_prov_zip_cd                string,
        diag_onset_dt                         date,
        diag_resltn_dt                        date,
        diag_cd                               string,
        diag_cd_qual                          string,
        diag_alt_cd                           string,
        diag_alt_cd_qual                      string,
        diag_nm                               string,
        diag_desc                             string,
        diag_prty_cd                          string,
        diag_prty_cd_qual                     string,
        diag_svty_cd                          string,
        diag_svty_cd_qual                     string,
        diag_resltn_cd                        string,
        diag_resltn_cd_qual                   string,
        diag_resltn_nm                        string,
        diag_resltn_desc                      string,
        diag_stat_cd                          string,
        diag_stat_cd_qual                     string,
        diag_stat_nm                          string,
        diag_stat_desc                        string,
        diag_snomed_cd                        string,
        diag_meth_cd                          string,
        diag_meth_cd_qual                     string,
        diag_meth_nm                          string,
        diag_meth_desc                        string,
        data_captr_dt                         date,
        rec_stat_cd                           string,
        prmy_src_tbl_nm                       string
        )
    {properties}
    ;
