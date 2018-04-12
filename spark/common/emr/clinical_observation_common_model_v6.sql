DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
        row_id                                     bigint,
        hv_clin_obsn_id                            string,
        crt_dt                                     date,
        mdl_vrsn_num                               string,
        data_set_nm                                string,
        src_vrsn_id                                string,
        hvm_vdr_id                                 int,
        hvm_vdr_feed_id                            int,
        vdr_org_id                                 string,
        vdr_clin_obsn_id                           string,
        vdr_clin_obsn_id_qual                      string,
        vdr_alt_clin_obsn_id                       string,
        vdr_alt_clin_obsn_id_qual                  string,
        hvid                                       string,
        ptnt_birth_yr                              int,
        ptnt_age_num                               string,
        ptnt_lvg_flg                               string,
        ptnt_dth_dt                                date,
        ptnt_gender_cd                             string,
        ptnt_state_cd                              string,
        ptnt_zip3_cd                               string,
        hv_enc_id                                  string,
        enc_dt                                     date,
        clin_obsn_dt                               date,
        clin_obsn_rndrg_fclty_npi                  string,
        clin_obsn_rndrg_fclty_vdr_id               string,
        clin_obsn_rndrg_fclty_vdr_id_qual          string,
        clin_obsn_rndrg_fclty_alt_id               string,
        clin_obsn_rndrg_fclty_alt_id_qual          string,
        clin_obsn_rndrg_fclty_tax_id               string,
        clin_obsn_rndrg_fclty_state_lic_id         string,
        clin_obsn_rndrg_fclty_comrcl_id            string,
        clin_obsn_rndrg_fclty_nucc_taxnmy_cd       string,
        clin_obsn_rndrg_fclty_alt_taxnmy_id        string,
        clin_obsn_rndrg_fclty_alt_taxnmy_id_qual   string,
        clin_obsn_rndrg_fclty_mdcr_speclty_cd      string,
        clin_obsn_rndrg_fclty_alt_speclty_id       string,
        clin_obsn_rndrg_fclty_alt_speclty_id_qual  string,
        clin_obsn_rndrg_fclty_nm                   string,
        clin_obsn_rndrg_fclty_addr_1_txt           string,
        clin_obsn_rndrg_fclty_addr_2_txt           string,
        clin_obsn_rndrg_fclty_state_cd             string,
        clin_obsn_rndrg_fclty_zip_cd               string,
        clin_obsn_rndrg_prov_npi                   string,
        clin_obsn_rndrg_prov_vdr_id                string,
        clin_obsn_rndrg_prov_vdr_id_qual           string,
        clin_obsn_rndrg_prov_alt_id                string,
        clin_obsn_rndrg_prov_alt_id_qual           string,
        clin_obsn_rndrg_prov_tax_id                string,
        clin_obsn_rndrg_prov_state_lic_id          string,
        clin_obsn_rndrg_prov_comrcl_id             string,
        clin_obsn_rndrg_prov_upin                  string,
        clin_obsn_rndrg_prov_ssn                   string,
        clin_obsn_rndrg_prov_nucc_taxnmy_cd        string,
        clin_obsn_rndrg_prov_alt_taxnmy_id         string,
        clin_obsn_rndrg_prov_alt_taxnmy_id_qual    string,
        clin_obsn_rndrg_prov_mdcr_speclty_cd       string,
        clin_obsn_rndrg_prov_alt_speclty_id        string,
        clin_obsn_rndrg_prov_alt_speclty_id_qual   string,
        clin_obsn_rndrg_prov_frst_nm               string,
        clin_obsn_rndrg_prov_last_nm               string,
        clin_obsn_rndrg_prov_addr_1_txt            string,
        clin_obsn_rndrg_prov_addr_2_txt            string,
        clin_obsn_rndrg_prov_state_cd              string,
        clin_obsn_rndrg_prov_zip_cd                string,
        clin_obsn_onset_dt                         date,
        clin_obsn_resltn_dt                        date,
        clin_obsn_data_ctgy_cd                     string,
        clin_obsn_data_ctgy_cd_qual                string,
        clin_obsn_data_ctgy_nm                     string,
        clin_obsn_data_ctgy_desc                   string,
        clin_obsn_typ_cd                           string,
        clin_obsn_typ_cd_qual                      string,
        clin_obsn_typ_nm                           string,
        clin_obsn_typ_desc                         string,
        clin_obsn_substc_cd                        string,
        clin_obsn_substc_cd_qual                   string,
        clin_obsn_substc_nm                        string,
        clin_obsn_substc_desc                      string,
        clin_obsn_cd                               string,
        clin_obsn_cd_qual                          string,
        clin_obsn_nm                               string,
        clin_obsn_desc                             string,
        clin_obsn_diag_cd                          string,
        clin_obsn_diag_cd_qual                     string,
        clin_obsn_diag_nm                          string,
        clin_obsn_snomed_cd                        string,
        clin_obsn_result_cd                        string,
        clin_obsn_result_cd_qual                   string,
        clin_obsn_result_nm                        string,
        clin_obsn_result_desc                      string,
        clin_obsn_msrmt                            string,
        clin_obsn_uom                              string,
        clin_obsn_qual                             string,
        clin_obsn_abnorm_flg                       string,
        clin_obsn_norm_min_msrmt                   string,
        clin_obsn_norm_max_msrmt                   string,
        data_src_cd                                string,
        data_captr_dt                              date,
        rec_stat_cd                                string,
        prmy_src_tbl_nm                            string
        {additional_columns}
        )
    {properties}
    ;