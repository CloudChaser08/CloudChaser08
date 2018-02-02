import spark.helpers.privacy.common as priv_common
from pyspark.sql.functions import md5

pharmacy_transformer = priv_common.Transformer(
    rx_number={
        'func': [md5],
        'args': [['rx_number']],
        'built-in': [True]
    }
)


def filter(df):
    return priv_common.filter(df, pharmacy_transformer)
