import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

diagnosis_transformer = {
    'diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_cd', 'diag_cd_qual', 'diag_dt']
    },
    'diag_alt_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_alt_cd', 'diag_alt_cd_qual', 'diag_dt']
    },
    'diag_prty_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_prty_cd', 'diag_prty_cd_qual', 'diag_dt']
    },
    'diag_svty_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_svty_cd', 'diag_svty_cd_qual', 'diag_dt']
    },
    'diag_stat_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_stat_cd', 'diag_stat_cd_qual', 'diag_dt']
    },
    'diag_meth_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['diag_meth_cd', 'diag_meth_cd_qual', 'diag_dt']
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
        'column_name': 'diag_meth_nm',
        'domain_name': 'emr_diag.diag_meth_nm'
    }
]

def filter(sqlc):
    def out(df):
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whitelists
            ]
        )(
            emr_priv_common.filter(df, diagnosis_transformer)
        )
    return out
