import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

clinical_observation_transformer = {
    'clin_obsn_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'enc_dt']
    }
}

whitelists = [
    {
        'column_name': 'clin_obsn_nm',
        'domain_name': 'emr_clin_obsn.clin_obsn_nm'
    },
    {
        'column_name': 'clin_obsn_diag_nm',
        'domain_name': 'emr_medctn.clin_obsn_diag_nm'
    },
    {
        'column_name': 'clin_obsn_diag_desc',
        'domain_name': 'emr_medctn.clin_obsn_diag_desc'
    },
    {
        'column_name': 'clin_obsn_result_desc',
        'domain_name': 'emr_clin_obsn.clin_obsn_result_desc'
    },
]


def filter(sqlc, update_whitelists=lambda x: x, additional_transforms=None):
    if not additional_transforms:
        additional_transforms = {}

    modified_transformer = dict(clinical_observation_transformer)
    modified_transformer.update(additional_transforms)

    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, modified_transformer)
        )
    return out
