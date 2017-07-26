import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

event_transformer = {
    'patient_year_of_birth': {
        'func': post_norm_cleanup.cap_year_of_birth,
        'args': ['patient_age', 'event_date', 'patient_year_of_birth']
    }
}

def filter(df):
    return priv_common.filter(df, event_transformer)
