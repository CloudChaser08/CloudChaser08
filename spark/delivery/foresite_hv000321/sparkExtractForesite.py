#! /usr/bin/python
import argparse
from spark.runner import Runner
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader
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

    PHARMACY_OUT_TEMPLATE = STAGING_DIR + '/pharmacy_claims_t2d'
    ENROLLMENT_OUT_TEMPLATE = STAGING_DIR + '/enrollment_t2d'

    runner.run_spark_script('reload_codes.sql', [
        ['analyticsdb_schema', FORESITE_SCHEMA, False]
    ])

    runner.run_spark_script('create_pharmacy_extract.sql', [
        ['analyticsdb_schema', FORESITE_SCHEMA, False],
        ['delivery_date', date]
    ])

    runner.run_spark_script('create_enrollment_extract.sql', [
        ['analyticsdb_schema', FORESITE_SCHEMA, False],
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
    spark, sqlContext = init("Foresite Capital Delivery")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(S3_FORESITE_DEST_TEMPLATE.format(args.date.replace('-', '')))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
