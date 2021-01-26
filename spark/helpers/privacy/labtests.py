import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

lab_transformer = priv_common.Transformer(
    loinc_code=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_numeric_code, ['loinc_code'])
    ],
    ordering_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['ordering_npi'])
    ],
    lab_npi=[
        priv_common.TransformFunction(post_norm_cleanup.clean_up_npi_code, ['lab_npi'])
    ],
    ordering_state=[
        priv_common.TransformFunction(post_norm_cleanup.validate_state_code, ['ordering_state'])
    ]
)


def filter(df, additional_transformer=None):
    return priv_common.filter(df, additional_transformer=lab_transformer.overwrite(additional_transformer))
