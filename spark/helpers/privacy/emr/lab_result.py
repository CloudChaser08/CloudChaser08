import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_result_transformer = {
    'lab_test_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['lab_test_diag_cd', 'lab_test_diag_cd_qual', 'lab_test_execd_dt']
    }
}

def filter(df):
    return emr_priv_common.filter(df, lab_result_transformer)
