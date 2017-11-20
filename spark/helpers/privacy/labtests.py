import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_transformer = {
    'loinc_code': {
        'func': post_norm_cleanup.clean_up_numeric_code,
        'args': ['loinc_code']
    },
    'ordering_npi': {
        'func': post_norm_cleanup.clean_up_npi_code,
        'args': ['ordering_npi']
    }
}

def filter(df):
    return priv_common.filter(df, additional_transforms=lab_transformer)
