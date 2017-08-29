import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

procedure_transformer = {
    'proc_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['proc_diag_cd', 'proc_diag_cd_qual', 'proc_dt']
    }
}

def filter(sqlc):
    def out(df):
        return emr_priv_common.filter(df, procedure_transformer)
    return out
