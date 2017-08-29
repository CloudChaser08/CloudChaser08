import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

clinical_observation_transformer = {
    'clin_obsn_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'clin_obsn_dt']
    }
}

whitelists = [
    {
        'column_name': 'clin_obsn_diag_nm',
        'domain_name': 'emr_medctn.clin_obsn_diag_nm'
    },
    {
        'column_name': 'clin_obsn_diag_desc',
        'domain_name': 'emr_medctn.clin_obsn_diag_desc'
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
            emr_priv_common.filter(df, clinical_observation_transformer)
        )
    return out
