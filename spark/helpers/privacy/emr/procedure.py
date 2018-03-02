from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.postprocessor as postprocessor

procedure_transformer = Transformer(
    proc_cd=[
        TransformFunction(post_norm_cleanup.clean_up_procedure_code, ['proc_cd'])
    ],
    proc_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['proc_diag_cd', 'proc_diag_cd_qual', 'proc_dt'])
    ]
)

whitelists = []


def filter(sqlc, update_whitelists=lambda x: x, additional_transformer=None):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(sqlc, whitelist['column_name'], whitelist['domain_name'])
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, procedure_transformer.overwrite(additional_transformer))
        )

    return out
