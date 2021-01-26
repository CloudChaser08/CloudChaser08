import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup

event_transformer = priv_common.Transformer(
    patient_age=[
        priv_common.TransformFunction(post_norm_cleanup.cap_age, ['patient_age']),
        priv_common.TransformFunction(post_norm_cleanup.validate_age,
                                      ['patient_age', 'event_date', 'patient_year_of_birth'])
    ],
    patient_year_of_birth=[
        priv_common.TransformFunction(
            post_norm_cleanup.cap_year_of_birth, ['patient_age', 'event_date', 'patient_year_of_birth']
        )
    ]
)


def filter(df):
    return priv_common.filter(df, event_transformer)
