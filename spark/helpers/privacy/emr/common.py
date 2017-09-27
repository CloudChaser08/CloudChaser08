import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

emr_transformer = {
    'ptnt_age_num': {
        'func': post_norm_cleanup.cap_age,
        'args': ['ptnt_age_num']
    },
    'ptnt_birth_yr': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['ptnt_age_num', 'enc_dt', 'ptnt_birth_yr']
    },
    'ptnt_zip3_cd': {
        'func': post_norm_cleanup.mask_zip_code,
        'args': ['ptnt_zip3_cd']
    }
}

def filter(df, additional_transforms={}):
    modified_emr_transformer = dict(emr_transformer)
    modified_emr_transformer.update(additional_transforms)
    return priv_common.filter(df, modified_emr_transformer)
