from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

clinical_observation_transformer = Transformer(
    clin_obsn_diag_cd=[
        TransformFunction(
            post_norm_cleanup.clean_up_diagnosis_code, ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'enc_dt']
        )
    ],
    clin_obsn_ndc=[
        TransformFunction(post_norm_cleanup.clean_up_ndc_code, ['clin_obsn_ndc'])
    ],
    clin_obsn_rndrg_prov_npi=[
        TransformFunction(
            post_norm_cleanup.clean_up_npi_code, ['clin_obsn_rndrg_prov_npi']
        )
    ],
    clin_obsn_rndrg_prov_state_cd=[
        TransformFunction(post_norm_cleanup.validate_state_code, ['clin_obsn_rndrg_prov_state_cd'])
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
                    comp_col_names=whitelist.get('comp_col_names'),
                    whitelist_col_name=whitelist.get('whitelist_col_name'),
                    clean_up_freetext_fn=whitelist.get('clean_up_freetext_fn')
                )
                for whitelist in whtlsts
            ]
        )(
            emr_priv_common.filter(df, clinical_observation_transformer.overwrite(additional_transformer))
        )
    return out
