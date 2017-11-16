import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

provider_order_transformer = {
}

whitelists = [
    {
        'column_name'           : 'prov_ord_alt_cd',
        'domain_name'           : 'emr_prov_ord.prov_ord_alt_cd',
        'comp_column_names'     : ['prov_ord_alt_cd_qual'],
        'whitelist_column_name' : 'gen_ref_cd'
    },
    {
        'column_name' : 'prov_ord_alt_nm',
        'domain_name' : 'emr_prov_ord.prov_ord_alt_nm'
        'clean_up_freetext' : False,
    },
    {
        'column_name'           : 'prov_ord_alt_desc',
        'domain_name'           : 'emr_prov_ord.prov_ord_alt_desc',
        'whitelist_column_name' : 'gen_ref_itm_desc'
    },
    {
        'column_name': 'prov_ord_diag_nm',
        'domain_name': 'emr_prov_ord.prov_ord_diag_nm'
    },
    {
        'column_name'           : 'prov_ord_rsn_cd',
        'domain_name'           : 'emr_prov_ord.prov_ord_rsn_cd',
        'comp_column_names'     : ['prov_ord_rsn_cd_qual'],
        'whitelist_column_name' : 'gen_ref_cd'
    },
    {
        'column_name': 'prov_ord_rsn_nm',
        'domain_name': 'emr_prov_ord.prov_ord_rsn_nm'
    },
    {
        'column_name'           : 'prov_ord_stat_cd',
        'domain_name'           : 'emr_prov_ord.prov_ord_stat_cd',
        'comp_column_names'     : ['prov_ord_stat_cd_qual'],
        'whitelist_column_name' : 'gen_ref_cd'
    },
    {
        'column_name': 'prov_ord_complt_rsn_cd',
        'domain_name': 'emr_prov_ord.prov_ord_complt_rsn_cd'
    },
    {
        'column_name': 'prov_ord_cxld_rsn_cd',
        'domain_name': 'emr_prov_ord.prov_ord_cxld_rsn_cd'
    },
    {
        'column_name': 'prov_ord_result_desc',
        'domain_name': 'emr_prov_ord.prov_ord_result_desc'
    },
    {
        'column_name'       : 'prov_ord_trtmt_typ_cd',
        'domain_name'       : 'emr_prov_ord.prov_ord_trtmt_typ_cd',
        'comp_column_names' : ['prov_ord_trtmt_typ_cd_qual']
    },
    {
        'column_name'           : 'prov_ord_rfrd_speclty_cd',
        'domain_name'           : 'emr_prov_ord.prov_ord_rfrd_speclty_cd',
        'comp_column_names'     : ['prov_ord_rfrd_speclty_cd_qual'],
        'whitelist_column_name' : 'gen_ref_cd'
    },
    {
        'column_name': 'prov_ord_specl_instrs_desc',
        'domain_name': 'emr_prov_ord.prov_ord_specl_instrs_desc'
    }
]

def filter(sqlc, update_whitelists=lambda x: x):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'],
                    whitelist['domain_name'], whitelist.get('comp_column_names'),
                    whitelist.get('clean_up_freetext'))
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, provider_order_transformer)
        )
    return out

