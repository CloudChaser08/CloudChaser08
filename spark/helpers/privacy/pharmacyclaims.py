import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.general_helpers as gen_helpers

pharmacy_transformer = {
    'rx_number': {
        'func': lambda x: gen_helpers.md5hash(x).lower() if x else None,
        'args': ['rx_number']
    }
}

def filter(df):
    return priv_common.filter(df, pharmacy_transformer)
