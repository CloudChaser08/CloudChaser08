import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

diagnosis_transformer = {
    'diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_cd', 'diag_cd_qual', 'diag_dt']
    }
}

whitelists = [
    {
        'column_name': 'diag_nm',
        'domain_name': 'emr_diag.diag_nm'
    },
    {
        'column_name': 'diag_desc',
        'domain_name': 'emr_diag.diag_desc'
    },
    {
        'column_name': 'diag_resltn_desc',
        'domain_name': 'emr_diag.diag_resltn_desc'
    },
    {
        'column_name': 'diag_stat_desc',
        'domain_name': 'emr_diag.diag_stat_desc'
    },
    {
        'column_name': 'diag_meth_nm',
        'domain_name': 'emr_diag.diag_meth_nm'
    }
]

def filter(sqlc, update_whitelists=lambda x: x):
    def out(df):
        whtlsts = update_whitelists(whtlsts)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, diagnosis_transformer)
        )
    return out
