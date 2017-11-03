import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_transformer = {
    'loinc_code': {
        'func': post_norm_cleanup.clean_up_numeric_code,
        'args': ['loinc_code']
    },
}

def filter(sqlc):
    def out(df):
        return priv_common.filter(df, sqlc=sqlc, additional_transforms=lab_transformer)
    return out
