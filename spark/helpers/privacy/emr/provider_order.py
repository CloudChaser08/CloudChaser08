from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor

provider_order_transformer = Transformer(
    prov_ord_diag_cd=[
        TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['prov_ord_diag_cd', 'prov_ord_diag_cd_qual', 'prov_ord_dt'])
    ],
    ordg_prov_state_cd=[
        TransformFunction(post_norm_cleanup.validate_state_code, ['ordg_prov_state_cd'])
    ]
)

whitelists = []

def filter(sqlc, update_whitelists=lambda x: x, additional_transformer=None):
    def out(df):
        whtlsts = update_whitelists(whitelists)
        return postprocessor.compose(
            *[
                postprocessor.apply_whitelist(
                    sqlc, whitelist['column_name'], whitelist['domain_name'],
                    whitelist.get('comp_col_names'), whitelist.get('whitelist_col_name'),
                    whitelist.get('clean_up_freetext_fn')
                )
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, provider_order_transformer.overwrite(additional_transformer))
        )
    return out
