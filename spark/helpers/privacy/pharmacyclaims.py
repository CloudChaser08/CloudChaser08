import spark.helpers.privacy.common as priv_common
import spark.helpers.udf.general_helpers as gen_helpers

pharmacy_transformer = {
    'rx_number': {
        'func': gen_helpers.md5,
        'args': ['rx_number']
    }
}

def filter(df, pharmacy_transformer):
    return priv_common.filter(df)
