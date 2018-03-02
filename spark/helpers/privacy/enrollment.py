import spark.helpers.privacy.common as priv_common

enrollment_transformer = priv_common.Transformer()


def filter(df):
    return priv_common.filter(df, enrollment_transformer)
