from datetime import datetime, date
from functools import reduce
import argparse

import pyspark.sql.functions as F
from pyspark.sql import Window

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims_common_model import schema_v4 as schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as med_priv
import spark.providers.xifin.medicalclaims.transactions_loader as transactions_loader

FEED_ID = '55'
VENDOR_ID = '239'

def run(spark, runner, date_input, in_parts=False, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/xifin/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/xifin/medicalclaims/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/xifin/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/xifin/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/medicalclaims/xifin/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/medicalclaims/xifin/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    explode.generate_exploder_table(spark, 2, 'proc_test_exploder')
    explode.generate_exploder_table(spark, 5, 'proc_diag_exploder')
    explode.generate_exploder_table(spark, 5, 'claim_transaction_amount_exploder')
    min_date = postprocessor.coalesce_dates(
                    runner.sqlContext,
                    FEED_ID,
                    None,
                    'EARLIEST_VALID_SERVICE_DATE'
                )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    transactions_loader.load(runner, input_path)
    transactions_loader.load_matching_payloads(runner, matching_path)

    parts = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'] if in_parts else [None]

    for prt in parts:
        transactions_loader.reconstruct_records(runner, partitions=(
            10 if test else 2500
        ), part=prt)

        normalized = reduce(
            lambda df1, df2: df1.union(df2), [
                schema_enforcer.apply_schema(
                    runner.run_spark_script(
                        file_utils.get_abs_path(script_path, script), [
                            ['min_date', '1900-01-01']
                        ], return_output=True
                    ), schema, columns_to_keep=['diagnosis_priority_unranked']
                ) for script in [
                    'normalize_1.sql', 'normalize_2.sql', 'normalize_3.sql'
                ]
            ]
        ).distinct()

        normalized_with_priority_rank = normalized.where(F.col("diagnosis_priority_unranked").isNotNull()).withColumn(
            'diagnosis_priority', F.dense_rank().over(
                Window.partitionBy("vendor_test_id", "claim_id").orderBy("diagnosis_priority_unranked")
            )
        ).unionAll(
            normalized.where(F.col("diagnosis_priority_unranked").isNull()).withColumn(
                'diagnosis_priority', F.lit(None)
            )
        ).drop("diagnosis_priority_unranked")

        postprocessed = postprocessor.compose(
            schema_enforcer.apply_schema_func(schema),
            postprocessor.add_universal_columns(
                feed_id=FEED_ID,
                vendor_id=VENDOR_ID,
                filename=None,
                model_version_number='04'
            ),
            postprocessor.trimmify, postprocessor.nullify,
            postprocessor.apply_date_cap(
                runner.sqlContext,
                'date_service',
                max_date,
                FEED_ID,
                None,
                min_date
            ),
            schema_enforcer.apply_schema_func(schema),
            lambda df: med_priv.filter(df, skip_pos_filter=True),
            schema_enforcer.apply_schema_func(schema)
        )(normalized_with_priority_rank)

        if not test:
            hvm_historical = postprocessor.coalesce_dates(
                runner.sqlContext,
                FEED_ID,
                date(1900, 1, 1),
                'HVM_AVAILABLE_HISTORY_START_DATE',
                'EARLIST_VALID_SERVICE_DATE'
            )

            prefix = date_input if prt is None else date_input + '_' + prt
            normalized_records_unloader.unload(
                spark, runner, postprocessed, 'date_service', date_input, 'xifin',
                hvm_historical_date=datetime(hvm_historical.year,
                                            hvm_historical.month,
                                            hvm_historical.day)
            )
        else:
            postprocessed.createOrReplaceTempView('xifin_medicalclaims')


def main(args):
    spark, sqlContext = init('Xifin')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, in_parts = args.in_parts, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/xifin/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/medicalclaims/2018-06-06/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--in_parts', default=False, action='store_true')
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
