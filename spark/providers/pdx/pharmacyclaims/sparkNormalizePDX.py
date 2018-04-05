import argparse
import subprocess
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims_common_model_v6 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
from pyspark.sql.functions import col, lit, upper

def run(spark, runner, date_input, test=False, airflow_test=False):
    setid = 'hvfeedfile_po_record_deid_{}.hvout'.format(date_input.replace('-', ''))

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/pdx/pharmacyclaims/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/pdx/pharmacyclaims/resources/matching/'
        )
        normalized_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/pdx/pharmacyclaims/resources/normalized/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/normalized/'
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )
        normalized_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/part_provider=pdx/'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        '65',
        None,
        'EARLIEST_VALID_SERVICE_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    payload_loader.load(runner, matching_path, ['claimId', 'patientId', 'hvJoinKey'])

    import spark.providers.pdx.pharmacyclaims.load_transactions as load_transactions
    load_transactions.load(runner, input_path)

    # Dedupe the source data
    runner.sqlContext.sql('select * from pdx_transactions') \
          .dropDuplicates(map(lambda x: x[0], load_transactions.TABLES['pdx_transactions'][:-1])) \
          .createOrReplaceTempView('pdx_transactions')

    # Normalize the source data
    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [['date_input', date_input]],
        return_output=True
    )

    # Post-processing on the normalized data
    postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id='65',
            vendor_id='262',
            filename=setid,
            model_version_number='6'
        ),
        postprocessor.nullify,
        postprocessor.apply_date_cap(
            runner.sqlContext,
            'date_service',
            max_date,
            '65',
            None,
            min_date
        ),
        pharm_priv.filter
    )(
        normalized_df
    ).createOrReplaceTempView('pharmacyclaims_common_model')

    # Run the reversal logic on the normalized source and
    # the last 2 months of normalized data
    current_year_month = date_input[:7]
    prev_year_month = (datetime.strptime(date_input, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m')
    if not test:
        spark.read.parquet(normalized_path + '/part_best_date={}'.format(current_year_month),
                           normalized_path + '/part_best_date={}'.format(prev_year_month)
                          ).createOrReplaceTempView('normalized_claims')
    else:
        spark.read.csv([normalized_path + '/part_best_date={}'.format(current_year_month),
                        normalized_path + '/part_best_date={}'.format(prev_year_month)],
                       schema=schema,
                       sep='|'
                      ).createOrReplaceTempView('normalized_claims')

    new_reversals = runner.sqlContext \
            .sql("select * from pharmacyclaims_common_model" + \
                 " where logical_delete_reason = 'Reversal'")
    new_not_reversed = runner.sqlContext \
            .sql("select * from pharmacyclaims_common_model" + \
                 " where logical_delete_reason is null or logical_delete_reason <> 'Reversal'")
    old_reversals = runner.sqlContext \
            .sql("select * from normalized_claims where logical_delete_reason = 'Reversal'")
    old_not_reversed = runner.sqlContext \
            .sql("select * from normalized_claims where logical_delete_reason is null or logical_delete_reason <> 'Reversal'")

    not_reversed = new_not_reversed.union(old_not_reversed)

    join_conditions = [
        upper(not_reversed.response_code_vendor) == lit('D'),
        ((upper(not_reversed.payer_type) == lit('MEDICAID')) | (upper(not_reversed.payer_type) == lit('THIRD PARTY'))),
        not_reversed.submitted_ingredient_cost == new_reversals.submitted_ingredient_cost,
        (not_reversed.paid_patient_pay + not_reversed.paid_gross_due) == (new_reversals.paid_patient_pay + new_reversals.paid_gross_due),
        not_reversed.rx_number == new_reversals.rx_number,
        not_reversed.ndc_code == new_reversals.ndc_code,
        not_reversed.pharmacy_other_id == new_reversals.pharmacy_other_id,
        not_reversed.date_service == new_reversals.date_service,
        not_reversed.logical_delete_reason.isNull()
    ]
    reversed_claims = not_reversed.join(new_reversals, join_conditions, 'leftsemi') \
                                  .withColumn('logical_delete_reason', lit('Reversed Claim'))
    unchanged_claims = not_reversed.join(new_reversals, join_conditions, 'leftanti')

    new_reversals.union(old_reversals).union(reversed_claims).union(unchanged_claims) \
                 .createOrReplaceTempView('pharmacyclaims_common_model_final')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            '65',
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v6.sql',
            'pdx', 'pharmacyclaims_common_model_final',
            'date_service', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )


def main(args):
    spark, sqlContext = init('PDX Normalization')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/spark-output/'
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/temp/'
    else:
        output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'
        tmp_path = 's3://salus/tmp/pdx/{}/'.format(args.date.replace('-', '/'))

    current_year_month = args.date[:7]
    prev_year_month = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m')

    date_part = 'part_best_date={}/'
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', output_path + date_part.format(current_year_month), tmp_path + date_part.format(current_year_month)]
    )
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', output_path + date_part.format(prev_year_month), tmp_path + date_part.format(prev_year_month)]
    )
    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
