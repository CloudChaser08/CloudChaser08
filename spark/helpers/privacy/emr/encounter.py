import spark.helpers.privacy.emr.common as emr_priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

encounter_transformer = {
    'ptnt_birth_yr': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['ptnt_age_num', 'enc_start_dt', 'ptnt_birth_yr']
    }
}

def filter(df):
    return emr_priv_common.filter(df, encounter_transformer)
