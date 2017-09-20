import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

procedure_transformer = {
    'proc_cd': {
        'func': post_norm_cleanup.clean_up_procedure_code,
        'args': ['proc_cd']
    },
    'proc_diag_cd': {
        'func': post_norm_cleanup.clean_up_diagnosis_code,
        'args': ['proc_diag_cd', 'proc_diag_cd_qual', 'proc_dt']
    }
}

whitelists = []

def filter(sqlc, update_whitelists=lambda x: x):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, procedure_transformer)
        )
    return out
