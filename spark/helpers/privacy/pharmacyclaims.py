import spark.helpers.privacy.common as priv_common
from pyspark.sql.functions import md5

pharmacy_transformer = priv_common.Transformer(
    rx_number=[
        priv_common.TransformFunction(md5, ['rx_number'], True)
    ]
)


def filter(df):
    return priv_common.filter(df, pharmacy_transformer)
