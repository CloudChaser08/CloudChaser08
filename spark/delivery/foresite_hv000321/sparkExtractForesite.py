"""extract forcesite"""
#! /usr/bin/python
import argparse
from spark.runner import Runner
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.file_utils as file_utils
import spark.helpers.extractor as extractor
from spark.spark_setup import init

script_path = __file__

FORESITE_SCHEMA = 'for321'

S3_FORESITE_DEST_TEMPLATE = 's3://salusv/projects/foresite_capital/hv000321/delivery/{}'


def run(spark, runner, date, test=False):

    if test:
        STAGING_DIR = '../../test/delivery/foresite_hv000321/resources/tmp/{}'
    else:
        STAGING_DIR = constants.hdfs_staging_dir

        external_table_loader.load_analytics_db_tables(
            runner.sqlContext, [
                {'schema': 'default', 'table_name': 'ref_ndc_code',
                 'local_alias': 'external_ref_ndc_code'},
                {'schema': 'default', 'table_name': 'ref_icd10_diagnosis',
                 'local_alias': 'external_ref_icd10_diagnosis'},
                {'schema': 'default', 'table_name': 'ref_marketplace_to_warehouse',
                 'local_alias': 'external_ref_marketplace_to_warehouse'},
                {'schema': 'default', 'table_name': 'ref_calendar',
                 'local_alias': 'external_ref_calendar'},
                {'schema': 'default', 'table_name': 'pharmacyclaims',
                 'local_alias': 'external_pharmacyclaims'},
                {'schema': 'default', 'table_name': 'enrollmentrecords',
                 'local_alias': 'external_enrollmentrecords'},
                {'schema': FORESITE_SCHEMA, 'table_name': 'mkt_def_calendar',
                 'local_alias': 'external_mkt_def_calendar'},
            ]
        )

    PHARMACY_OUT_TEMPLATE = STAGING_DIR + '/pharmacy_claims_t2d'
    ENROLLMENT_OUT_TEMPLATE = STAGING_DIR + '/enrollment_t2d'

    runner.sqlContext.sql('CREATE DATABASE IF NOT EXISTS for321')

    runner.run_spark_script('reload_codes.sql', [
        ['foresite_schema', FORESITE_SCHEMA, False]
    ])

    runner.run_spark_script('create_pharmacy_extract.sql', [
        ['foresite_schema', FORESITE_SCHEMA, False],
        ['delivery_date', date]
    ])

    runner.run_spark_script('create_enrollment_extract.sql', [
        ['foresite_schema', FORESITE_SCHEMA, False],
        ['delivery_date', date]
    ])

    extractor.export_table(
        runner.sqlContext, 'pharmacy_claims_t2d', FORESITE_SCHEMA,
        file_utils.get_abs_path(
            script_path, PHARMACY_OUT_TEMPLATE.format(date.replace('-', ''))
        )
    )

    extractor.export_table(
        runner.sqlContext, 'enrollment_t2d', FORESITE_SCHEMA,
        file_utils.get_abs_path(
            script_path, ENROLLMENT_OUT_TEMPLATE.format(date.replace('-', ''))
        )
    )


def main(args):
    # init
    spark, sql_context = init("Foresite Capital Delivery")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(S3_FORESITE_DEST_TEMPLATE.format(args.date.replace('-', '')))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    main(args)
