from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_order_transformer = Transformer(
    lab_ord_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                          ['lab_ord_diag_cd', 'lab_ord_diag_cd_qual', 'enc_dt'])
    ],
    lab_ord_loinc_cd=[
        TransformFunction(post_norm_cleanup.clean_up_numeric_code, ['lab_ord_loinc_cd'])
    ],
    lab_ord_ordg_prov_state_cd=[
        TransformFunction(post_norm_cleanup.validate_state_code, ['lab_ord_ordg_prov_state_cd'])
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
            emr_priv_common.filter(df, lab_order_transformer.overwrite(additional_transformer))
        )
    return out
