from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

vital_sign_transformer = Transformer(
    vit_sign_msrmt=[
        TransformFunction(post_norm_cleanup.clean_up_vital_sign,[
            'vit_sign_typ_cd', 'vit_sign_msrmt', 'vit_sign_uom', 'ptnt_gender_cd',
            'ptnt_age_num', 'ptnt_birth_yr', 'data_captr_dt', 'enc_dt'
        ])
    ]
)

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
            emr_priv_common.filter(df, vital_sign_transformer)
        )
    return out
