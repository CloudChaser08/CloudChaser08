import argparse

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils

def run(runner, date_input, test=False, airflow_test=False):
    input_tables = [
        'svc',
        'ts3',
        'hdr',
        'plb',
        'clp',
        'payload'
    ]

    input_paths = {}
    if test:
        script_path = __file__
        for t in input_tables:
            if t == 'payload':
                input_paths[t] = file_utils.get_abs_path(
                    script_path, '../../../test/providers/allscripts/custom_era/resources/matching/'
                )
            else:
                input_paths[t] = file_utils.get_abs_path(
                    script_path, '../../../test/providers/genomind/labtests/resources/input/{}/'.format(t)
                )
    elif airflow_test:
        for t in input_tables:
            if t == 'payload':
                input_paths[t] = 's3://salusv/testing/dewey/airflow/e2e/allscripts_era/matching/'
            else:
                input_paths[t] = 's3://salusv/testing/dewey/airflow/e2e/allscripts_era/input/{}'.format(t)
    else:
        for t in input_tables:
            if t == 'payload':
                input_paths[t] = 's3://salusv/matching/payload/era/allscripts/{}/'.format(
                    date_input.replace('-', '/')
                )
            else:
                input_paths[t] = 's3://salusv/incoming/incoming/era/{}/{}/'.format(
                    date_input.replace('-', '/'), t
                )

    import spark.providers.allscripts.custom_era.load_transactions as load_transactions
    load_transactions.load(runner, input_paths)

    output_locations = {
        'svc': 's3://salusv/warehouse/parquet/era_allscripts_svc/',
        'ts3': 's3://salusv/warehouse/parquet/era_allscripts_ts3/',
        'hdr': 's3://salusv/warehouse/parquet/era_allscripts_hdr/',
        'plb': 's3://salusv/warehouse/parquet/era_allscripts_plb/',
        'clp': 's3://salusv/warehouse/parquet/era_allscripts_clp/',
        'payload': 's3://salusv/warehouse/parquet/era_allscripts_payload/'
    }

    if not test:
        for table, output_location in output_locations.items():
            runner.sqlContext.sql('select * from {}'.format(table)) \
                             .repartition(20) \
                             .write.parquet(output_location)

def main(args):
    spark, sqlContext = init('Allscripts ERA Custom Normalization')

    runner = Runner(sqlContext)

    run(runner, args.date, airflow_test=args.airflow_test)

    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
