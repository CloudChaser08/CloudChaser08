DROP TABLE IF EXISTS transactional_raw;
CREATE EXTERNAL TABLE transactional_raw (
        mystery_column                 string,
        src_claim_id                   string,
        claim_type_cd                  string,
        edi_interchange_creation_dt    string,
        edi_interchange_creation_time  string,
        orgntr_app_txn_id              string,
        billg_provdr_txnmy             string,
        billg_provdr_npi               string,
        billg_provdr_tax_id            string,
        billg_provdr_stlc_nbr          string,
        billg_provdr_upin              string,
        billg_provdr_last_or_orgal_nm  string,
        billg_provdr_first_nm          string,
        billg_provdr_addr_1            string,
        billg_provdr_addr_2            string,
        billg_provdr_addr_city         string,
        billg_provdr_addr_state        string,
        billg_provdr_addr_zip          string,
        pay_to_provdr_addr_1           string,
        pay_to_provdr_addr_2           string,
        pay_to_provdr_city             string,
        pay_to_provdr_state            string,
        pay_to_provdr_zip              string,
        dest_payer_nm                  string,
        dest_payer_cms_plan_id         string,
        dest_payer_id                  string,
        dest_payer_claim_offc_nbr      string,
        dest_payer_naic_id             string,
        dest_payer_tax_id              string,
        dest_payer_group_nbr           string,
        dest_payer_group_nm            string,
        dest_payer_claim_flng_ind_cd   string,
        tot_claim_charg_amt            string,
        fclty_type_pos_cd              string,
        claim_freq_cd                  string,
        asgmt_signtr                   string,
        release_signtr                 string,
        accdnt_related_ind             string,
        stmnt_from_dt                  string,
        stmnt_to_dt                    string,
        admsn_dt                       string,
        dischg_dt                      string,
        admsn_type_cd                  string,
        admsn_src_cd                   string,
        patnt_sts_cd                   string,
        prinpl_diag_cd                 string,
        prinpl_diag_prior_onset_ind    string,
        admtg_diag_cd                  string,
        patnt_rsn_for_visit_01         string,
        patnt_rsn_for_visit_02         string,
        patnt_rsn_for_visit_03         string,
        ecodes                         string,
        drg_cd                         string,
        dx                             string,
        prinpl_proc_cd                 string,
        other_proc_codes               string,
        refrn_provdr_npi               string,
        refrn_provdr_id                string,
        refrn_provdr_upin              string,
        refrn_provdr_comm_nbr          string,
        refrn_provdr_stlc_nbr          string,
        refrn_provdr_last_nm           string,
        refrn_provdr_first_nm          string,
        rendr_provdr_npi               string,
        rendr_provdr_id                string,
        rendr_provdr_upin              string,
        rendr_provdr_comm_nbr          string,
        rendr_provdr_loc_nbr           string,
        rendr_provdr_stlc_nbr          string,
        rendr_provdr_last_nm           string,
        rendr_provdr_first_nm          string,
        rendr_provdr_txnmy             string,
        fclty_npi                      string,
        fclty_id                       string,
        fclty_comm_nbr                 string,
        fclty_loc_nbr                  string,
        fclty_stlc_nbr                 string,
        fclty_nm                       string,
        fclty_addr_1                   string,
        fclty_addr_2                   string,
        fclty_addr_city                string,
        fclty_addr_state               string,
        fclty_addr_zip                 string,
        suprv_provdr_npi               string,
        suprv_provdr_upin              string,
        suprv_provdr_comm_nbr          string,
        suprv_provdr_loc_nbr           string,
        suprv_provdr_stlc_nbr          string,
        suprv_provdr_last_nm           string,
        suprv_provdr_first_nm          string,
        attdn_provdr_npi               string,
        attdn_provdr_id                string,
        attdn_provdr_upin              string,
        attdn_provdr_comm_nbr          string,
        attdn_provdr_loc_nbr           string,
        attdn_provdr_stlc_nbr          string,
        attdn_provdr_last_nm           string,
        attdn_provdr_first_nm          string,
        attdn_provdr_txnmy             string,
        oprtn_provdr_npi               string,
        oprtn_provdr_id                string,
        oprtn_provdr_upin              string,
        oprtn_provdr_comm_nbr          string,
        oprtn_provdr_loc_nbr           string,
        oprtn_provdr_stlc_nbr          string,
        oprtn_provdr_last_nm           string,
        oprtn_provdr_first_nm          string,
        other_oprtn_provdr_npi         string,
        other_oprtn_provdr_id          string,
        other_oprtn_provdr_upin        string,
        other_oprtn_provdr_comm_nbr    string,
        other_oprtn_provdr_loc_nbr     string,
        other_oprtn_provdr_stlc_nbr    string,
        other_oprtn_provdr_last_nm     string,
        other_oprtn_provdr_first_nm    string,
        other_diag_codes               string,
        src_svc_id                     string,
        line_nbr                       string,
        svc_from_dt                    string,
        svc_to_dt                      string,
        proc_cd_qual                   string,
        proc_cd                        string,
        proc_modfr_1                   string,
        proc_modfr_2                   string,
        proc_modfr_3                   string,
        proc_modfr_4                   string,
        line_charg                     string,
        units                          string,
        pos_cd                         string,
        diag_cd_1                      string,
        diag_cd_2                      string,
        diag_cd_3                      string,
        diag_cd_4                      string,
        revnu_cd                       string,
        tst_rslt_hgb                   string,
        tst_rslt_hematocrit            string,
        tst_rslt_epoetin               string,
        tst_rslt_creatinine            string,
        tst_rslt_hgt                   string,
        line_item_cntl_nbr             string,
        ndc                            string,
        ndc_unit_cnt                   string,
        ndc_uom                        string,
        rendr_provdr_npi_svc           string,
        rendr_provdr_txnmy_svc         string,
        purchase_svc_provdr_npi        string,
        fclty_npi_svc                  string,
        suprv_provdr_npi_svc           string,
        ordrg_provdr_npi               string,
        refrn_provdr_npi_svc           string,
        oprtn_provdr_npi_svc           string,
        other_oprtn_provdr_npi_svc     string,
        eightthirtyfive_src_era_claim_id               string,
        eightthirtyfive_src_claim_id                   string,
        eightthirtyfive_claim_type_cd                  string,
        eightthirtyfive_edi_interchange_creation_dt    string,
        eightthirtyfive_edi_interchange_creation_time  string,
        eightthirtyfive_txn_handling_cd                string,
        eightthirtyfive_tot_actual_provdr_paymt_amt    string,
        eightthirtyfive_credit_debit_fl_cd             string,
        eightthirtyfive_paymt_method_cd                string,
        eightthirtyfive_paymt_eff_dt                   string,
        eightthirtyfive_payer_claim_cntl_nbr           string,
        eightthirtyfive_payer_claim_cntl_nbr_origl     string,
        eightthirtyfive_payer_nm                       string,
        eightthirtyfive_payer_cms_plan_id              string,
        eightthirtyfive_payer_id                       string,
        eightthirtyfive_payer_naic_id                  string,
        eightthirtyfive_payer_group_nbr                string,
        eightthirtyfive_payer_claim_flng_ind_cd        string,
        eightthirtyfive_payee_tax_id                   string,
        eightthirtyfive_payee_npi                      string,
        eightthirtyfive_payee_nm                       string,
        eightthirtyfive_payee_addr_1                   string,
        eightthirtyfive_payee_addr_2                   string,
        eightthirtyfive_payee_addr_city                string,
        eightthirtyfive_payee_addr_state               string,
        eightthirtyfive_payee_addr_zip                 string,
        eightthirtyfive_rendr_provdr_npi               string,
        eightthirtyfive_rendr_provdr_upin              string,
        eightthirtyfive_rendr_provdr_comm_nbr          string,
        eightthirtyfive_rendr_provdr_loc_nbr           string,
        eightthirtyfive_rendr_provdr_stlc_nbr          string,
        eightthirtyfive_rendr_provdr_last_nm           string,
        eightthirtyfive_rendr_provdr_first_nm          string,
        eightthirtyfive_claim_sts_cd                   string,
        eightthirtyfive_tot_claim_charg_amt            string,
        eightthirtyfive_tot_paid_amt                   string,
        eightthirtyfive_patnt_rspbty_amt               string,
        eightthirtyfive_fclty_type_pos_cd              string,
        eightthirtyfive_claim_freq_cd                  string,
        eightthirtyfive_stmnt_from_dt                  string,
        eightthirtyfive_stmnt_to_dt                    string,
        eightthirtyfive_covrg_expired_dt               string,
        eightthirtyfive_claim_received_dt              string,
        eightthirtyfive_drg_cd                         string,
        eightthirtyfive_drg_amt                        string,
        eightthirtyfive_clm_cas                        string,
        eightthirtyfive_src_era_svc_id                 string,
        eightthirtyfive_src_svc_id                     string,
        eightthirtyfive_line_item_cntl_nbr             string,
        eightthirtyfive_rendr_provdr_npi_svc           string,
        eightthirtyfive_rendr_provdr_upin_svc          string,
        eightthirtyfive_rendr_provdr_comm_nbr_svc      string,
        eightthirtyfive_rendr_provdr_loc_nbr_svc       string,
        eightthirtyfive_rendr_provdr_stlc_nbr_svc      string,
        eightthirtyfive_adjtd_proc_cd_qual             string,
        eightthirtyfive_adjtd_proc_cd                  string,
        eightthirtyfive_adjtd_proc_modfr_1             string,
        eightthirtyfive_adjtd_proc_modfr_2             string,
        eightthirtyfive_adjtd_proc_modfr_3             string,
        eightthirtyfive_adjtd_proc_modfr_4             string,
        eightthirtyfive_line_item_charg_amt            string,
        eightthirtyfive_line_item_allowed_amt          string,
        eightthirtyfive_line_item_paid_amt             string,
        eightthirtyfive_revnu_cd                       string,
        eightthirtyfive_paid_units                     string,
        eightthirtyfive_submd_proc_cd_qual             string,
        eightthirtyfive_submd_proc_cd                  string,
        eightthirtyfive_submd_proc_modfr_1             string,
        eightthirtyfive_submd_proc_modfr_2             string,
        eightthirtyfive_submd_proc_modfr_3             string,
        eightthirtyfive_submd_proc_modfr_4             string,
        eightthirtyfive_origl_units_of_svc_cnt         string,
        eightthirtyfive_svc_from_dt                    string,
        eightthirtyfive_svc_to_dt                      string,
        eightthirtyfive_svc_cas                        string
        )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '|'
        )
    STORED AS TEXTFILE
    LOCATION {input_path}
;

CACHE TABLE transactional_raw;
