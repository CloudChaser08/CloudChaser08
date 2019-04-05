import spark.helpers.postprocessor as postprocessor
import spark.helpers.records_loader as records_loader

def load(runner, input_path_prefix, actives_path):
    '''
    Load in the transactions to an in memory table.
    '''
    transactions_df = records_loader.load(runner, input_path_prefix, TABLES['alliance_transactions'], 'csv')
    actives_df = records_loader.load(runner, actives_path, TABLES['actives'], 'csv')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(transactions_df).createOrReplaceTempView('alliance_transactions')

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(actives_df).distinct().createOrReplaceTempView('actives')


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
    ],
    'actives': [
        'agility_id',
        'column2',
        'column3',
        'column4',
        'column5'
    ]
}
