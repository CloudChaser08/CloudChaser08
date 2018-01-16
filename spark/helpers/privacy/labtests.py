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
    },
    'ordering_state': {
        'func': post_norm_cleanup.validate_state_code,
        'args': ['ordering_state']
    }
}

def filter(df, additional_transforms=None):
    if not additional_transforms:
        additional_transforms = {}

    modified_lab_transformer = dict(lab_transformer)
    modified_lab_transformer.update(additional_transforms)

    return priv_common.filter(df, additional_transforms=modified_lab_transformer)
