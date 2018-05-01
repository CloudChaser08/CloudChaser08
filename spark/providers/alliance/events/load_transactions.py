import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_path_prefix):
    '''
    Load in the transactions to an in memory table.
    '''
    df = records_loader.load(runner, input_path_prefix, TABLES['alliance_transactions'])

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(df).createOrReplaceTempView('alliance_transactions')


TABLES = {
    'alliance_transactions': [
        'agility_id',
        'transaction_date',
        'transaction_usd',
        'merchant_name',
        'merchant_address',
        'merchant_city',
        'coalesce',
        'zip5',
        'zip4',
        'mcc_code',
        'mcc_desc',
        'naics_code',
        'naics_desc',
        'cnp_ind'
    ]
}
