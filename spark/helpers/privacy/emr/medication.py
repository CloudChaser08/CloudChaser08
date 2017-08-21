import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

medication_transformer = {
    'medctn_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['medctn_diag_cd', 'medctn_diag_cd_qual', 'medctn_admin_dt']
    }
}

def filter(df):
    return emr_priv_common.filter(df, medication_transformer)
