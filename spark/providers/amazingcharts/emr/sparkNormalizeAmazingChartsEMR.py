import argparse
from datetime import date, datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, rank

FEED_ID = '5'

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    input_tables = [
        'd_costar',
        'd_cpt',
        'd_drug',
        'd_icd10',
        'd_icd9',
        'd_lab_directory',
        'd_multum_to_ndc',
        'd_patient',
        'd_provider',
        'd_vaccine_cpt',
        'f_diagnosis',
        'f_encounter',
        'f_injection',
        'f_lab',
        'f_medication',
        'f_procedure'
    ]
    input_paths = {}
    if test:
        for t in input_tables:
            input_paths[t] = file_utils.get_abs_path(
                script_path, '../../../test/providers/amazingcharts/emr/resources/input/{}/'.format(t)
            )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/amazingcharts/emr/resources/payload/'
        )
    elif airflow_test:
        for t in input_tables:
            input_paths[t] = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/input/{}/{}/'.format(
                date_input.replace('-', '/'), t
            )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        for t in input_tables:
            input_paths[t] = 's3://salusv/incoming/consumer/alliance/{}/{}/'.format(
                date_input.replace('-', '/'), t
            )
        matching_path = 's3://salusv/matching/payload/consumer/alliance/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        None,
        'EARLIEST_VALID_SERVICE_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    import spark.providers.amazingcharts.emr.load_transactions as load_transactions
    load_transactions.load(runner, input_paths)

    payload_loader.load(runner, matching_path, ['personid'])
    # De-duplicate the payloads so that there is only one
    # personid (we use personid as our join key, not hvJoinKey)
    window = Window.partitionBy(col('personid')).orderBy(col('age'))
    runner.sqlContext.sql('select * from matching_payload')     \
          .withColumn('rank', rank().over(window))              \
          .where(col('rank') == lit(1))                         \
          .dropColumn('rank')                                   \
          .createOrReplaceTempView('matching_payload_deduped')


def main(args):
    spark, sqlContext = init('AmazingCharts EMR Normalization')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        #TODO: output each normalized table
        pass
    else:
        #TODO: output each normalized table
        pass

    #normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

