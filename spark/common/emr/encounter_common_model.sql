DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        rec_id                               bigint,
        hv_enc_id                            string,
        crt_dt                               date,
        mdl_vrsn_num                         string,
        data_set_nm                          string,
        src_vrsn_id                          string,
        hvm_vdr_id                           int,
        hvm_vdr_feed_id                      int,
        vdr_org_id                           string,
        vdr_enc_id                           string,
        vdr_enc_id_qual                      string,
        vdr_alt_enc_id                       string,
        vdr_alt_enc_id_qual                  string,
        hvid                                 string,
        ptnt_birth_yr                        int,
        ptnt_age_num                         string,
        ptnt_lvg_flg                         string,
        ptnt_dth_dt                          date,
        ptnt_gender_cd                       string,
        ptnt_state_cd                        string,
        ptnt_zip3_cd                         string,
        enc_start_dt                         date,
        enc_end_dt                           date,
        enc_vst_typ_cd                       string,
        enc_rndrg_fclty_npi                  string,
        enc_rndrg_fclty_vdr_id               string,
        enc_rndrg_fclty_vdr_id_qual          string,
        enc_rndrg_fclty_alt_id               string,
        enc_rndrg_fclty_alt_id_qual          string,
        enc_rndrg_fclty_tax_id               string,
        enc_rndrg_fclty_dea_id               string,
        enc_rndrg_fclty_state_lic_id         string,
        enc_rndrg_fclty_comrcl_id            string,
        enc_rndrg_fclty_nucc_taxnmy_cd       string,
        enc_rndrg_fclty_alt_taxnmy_id        string,
        enc_rndrg_fclty_alt_taxnmy_id_qual   string,
        enc_rndrg_fclty_mdcr_speclty_cd      string,
        enc_rndrg_fclty_alt_speclty_id       string,
        enc_rndrg_fclty_alt_speclty_id_qual  string,
        enc_rndrg_fclty_fclty_nm             string,
        enc_rndrg_fclty_addr_1_txt           string,
        enc_rndrg_fclty_addr_2_txt           string,
        enc_rndrg_fclty_state_cd             string,
        enc_rndrg_fclty_zip_cd               string,
        enc_rndrg_prov_npi                   string,
        enc_rndrg_prov_vdr_id                string,
        enc_rndrg_prov_vdr_id_qual           string,
        enc_rndrg_prov_alt_id                string,
        enc_rndrg_prov_alt_id_qual           string,
        enc_rndrg_prov_tax_id                string,
        enc_rndrg_prov_dea_id                string,
        enc_rndrg_prov_state_lic_id          string,
        enc_rndrg_prov_comrcl_id             string,
        enc_rndrg_prov_upin                  string,
        enc_rndrg_prov_ssn                   string,
        enc_rndrg_prov_nucc_taxnmy_cd        string,
        enc_rndrg_prov_alt_taxnmy_id         string,
        enc_rndrg_prov_alt_taxnmy_id_qual    string,
        enc_rndrg_prov_mdcr_speclty_cd       string,
        enc_rndrg_prov_alt_speclty_id        string,
        enc_rndrg_prov_alt_speclty_id_qual   string,
        enc_rndrg_prov_frst_nm               string,
        enc_rndrg_prov_last_nm               string,
        enc_rndrg_prov_addr_1_txt            string,
        enc_rndrg_prov_addr_2_txt            string,
        enc_rndrg_prov_state_cd              string,
        enc_rndrg_prov_zip_cd                string,
        enc_rfrg_prov_npi                    string,
        enc_rfrg_prov_vdr_id                 string,
        enc_rfrg_prov_vdr_id_qual            string,
        enc_rfrg_prov_alt_id                 string,
        enc_rfrg_prov_alt_id_qual            string,
        enc_rfrg_prov_tax_id                 string,
        enc_rfrg_prov_dea_id                 string,
        enc_rfrg_prov_state_lic_id           string,
        enc_rfrg_prov_comrcl_id              string,
        enc_rfrg_prov_upin                   string,
        enc_rfrg_prov_ssn                    string,
        enc_rfrg_prov_nucc_taxnmy_cd         string,
        enc_rfrg_prov_alt_taxnmy_id          string,
        enc_rfrg_prov_alt_taxnmy_id_qual     string,
        enc_rfrg_prov_mdcr_speclty_cd        string,
        enc_rfrg_prov_alt_speclty_id         string,
        enc_rfrg_prov_alt_speclty_id_qual    string,
        enc_rfrg_prov_fclty_nm               string,
        enc_rfrg_prov_frst_nm                string,
        enc_rfrg_prov_last_nm                string,
        enc_rfrg_prov_addr_1_txt             string,
        enc_rfrg_prov_addr_2_txt             string,
        enc_rfrg_prov_state_cd               string,
        enc_rfrg_prov_zip_cd                 string,
        enc_typ_cd                           string,
        enc_typ_cd_qual                      string,
        enc_typ_nm                           string,
        enc_desc                             string,
        enc_pos_cd                           string,
        data_captr_dt                        date,
        rec_stat_cd                          string,
        prmy_src_tbl_nm                      string
        )
    {properties}
;
