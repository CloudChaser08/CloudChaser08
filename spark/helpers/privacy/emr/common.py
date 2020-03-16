import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

emr_transformer = priv_common.Transformer(
    ptnt_age_num=[
        priv_common.TransformFunction(post_norm_cleanup.cap_age, ['ptnt_age_num']),
        priv_common.TransformFunction(post_norm_cleanup.validate_age, ['ptnt_age_num', 'enc_dt', 'ptnt_birth_yr'])
    ],
    ptnt_gender_cd=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_gender, ['ptnt_gender_cd'])
    ],
    ptnt_birth_yr=[
        priv_common.TransformFunction(post_norm_cleanup.cap_year_of_birth, ['ptnt_age_num', 'enc_dt', 'ptnt_birth_yr'])
    ],
    ptnt_zip3_cd=[
        priv_common.TransformFunction(post_norm_cleanup.mask_zip_code, ['ptnt_zip3_cd'])
    ],
    ptnt_state_cd=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['ptnt_state_cd'])
    ]
)


def filter(df, additional_transformer=None):
    return priv_common.filter(df, emr_transformer.overwrite(additional_transformer))
