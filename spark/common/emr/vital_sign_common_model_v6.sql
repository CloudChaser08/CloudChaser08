DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        row_id                                    bigint,
        hv_vit_sign_id                            string,
        crt_dt                                    date,
        mdl_vrsn_num                              string,
        data_set_nm                               string,
        src_vrsn_id                               string,
        hvm_vdr_id                                int,
        hvm_vdr_feed_id                           int,
        vdr_org_id                                string,
        vdr_vit_sign_id                           string,
        vdr_vit_sign_id_qual                      string,
        vdr_alt_vit_sign_id                       string,
        vdr_alt_vit_sign_id_qual                  string,
        hvid                                      string,
        ptnt_birth_yr                             int,
        ptnt_age_num                              string,
        ptnt_lvg_flg                              string,
        ptnt_dth_dt                               date,
        ptnt_gender_cd                            string,
        ptnt_state_cd                             string,
        ptnt_zip3_cd                              string,
        hv_enc_id                                 string,
        enc_dt                                    date,
        vit_sign_dt                               date,
        vit_sign_last_msrmt_dt                    date,
        vit_sign_rndrg_fclty_npi                  string,
        vit_sign_rndrg_fclty_vdr_id               string,
        vit_sign_rndrg_fclty_vdr_id_qual          string,
        vit_sign_rndrg_fclty_alt_id               string,
        vit_sign_rndrg_fclty_alt_id_qual          string,
        vit_sign_rndrg_fclty_tax_id               string,
        vit_sign_rndrg_fclty_state_lic_id         string,
        vit_sign_rndrg_fclty_comrcl_id            string,
        vit_sign_rndrg_fclty_nucc_taxnmy_cd       string,
        vit_sign_rndrg_fclty_alt_taxnmy_id        string,
        vit_sign_rndrg_fclty_alt_taxnmy_id_qual   string,
        vit_sign_rndrg_fclty_mdcr_speclty_cd      string,
        vit_sign_rndrg_fclty_alt_speclty_id       string,
        vit_sign_rndrg_fclty_alt_speclty_id_qual  string,
        vit_sign_rndrg_fclty_nm                   string,
        vit_sign_rndrg_fclty_addr_1_txt           string,
        vit_sign_rndrg_fclty_addr_2_txt           string,
        vit_sign_rndrg_fclty_state_cd             string,
        vit_sign_rndrg_fclty_zip_cd               string,
        vit_sign_rndrg_prov_npi                   string,
        vit_sign_rndrg_prov_vdr_id                string,
        vit_sign_rndrg_prov_vdr_id_qual           string,
        vit_sign_rndrg_prov_alt_id                string,
        vit_sign_rndrg_prov_alt_id_qual           string,
        vit_sign_rndrg_prov_tax_id                string,
        vit_sign_rndrg_prov_state_lic_id          string,
        vit_sign_rndrg_prov_comrcl_id             string,
        vit_sign_rndrg_prov_upin                  string,
        vit_sign_rndrg_prov_ssn                   string,
        vit_sign_rndrg_prov_nucc_taxnmy_cd        string,
        vit_sign_rndrg_prov_alt_taxnmy_id         string,
        vit_sign_rndrg_prov_alt_taxnmy_id_qual    string,
        vit_sign_rndrg_prov_mdcr_speclty_cd       string,
        vit_sign_rndrg_prov_alt_speclty_id        string,
        vit_sign_rndrg_prov_alt_speclty_id_qual   string,
        vit_sign_rndrg_prov_frst_nm               string,
        vit_sign_rndrg_prov_last_nm               string,
        vit_sign_rndrg_prov_addr_1_txt            string,
        vit_sign_rndrg_prov_addr_2_txt            string,
        vit_sign_rndrg_prov_state_cd              string,
        vit_sign_rndrg_prov_zip_cd                string,
        vit_sign_typ_cd                           string,
        vit_sign_typ_cd_qual                      string,
        vit_sign_typ_nm                           string,
        vit_sign_typ_desc                         string,
        vit_sign_snomed_cd                        string,
        vit_sign_msrmt                            string,
        vit_sign_uom                              string,
        vit_sign_qual                             string,
        vit_sign_abnorm_flg                       string,
        vit_sign_norm_min_msrmt                   string,
        vit_sign_norm_max_msrmt                   string,
        data_src_cd                               string,
        data_captr_dt                             date,
        rec_stat_cd                               string,
        prmy_src_tbl_nm                           string
        {additional_columns}
        )
    {properties}
    ;
