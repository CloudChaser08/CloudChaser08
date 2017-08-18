import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

clinical_observation_transformer = {
    'clin_obsn_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'clin_obsn_dt']
    }
}

def filter(df):
    return emr_priv_common.filter(df, clinical_observation_transformer)
