#! /usr/bin/python
import argparse
import time
from spark.runner import Runner
import spark.helpers.extractor as extractor
from spark.spark_setup import init

TODAY = time.strftime('%Y-%m-%d', time.localtime())

ANALYTICSDB_SCHEMA = 'for321'

S3_FORESITE_OUT = 's3://salusv/projects/foresite_capital/hv000321/delivery/'
S3_FORESITE_PHARMACY_OUT_TEMPLATE = S3_FORESITE_OUT + '{}/pharmacy_claims_t2d'
S3_FORESITE_ENROLLMENT_OUT_TEMPLATE = S3_FORESITE_OUT + '{}/enrollment_t2d'


def run(spark, runner, date, test=False):

    if test:
        S3_FORESITE_OUT = '../../test/delivery/foresite_hv-000321/resources/output/'
    else:
        S3_FORESITE_OUT = 's3://salusv/projects/foresite_capital/hv000321/delivery/'

    S3_FORESITE_PHARMACY_OUT_TEMPLATE = S3_FORESITE_OUT + '{}/pharmacy_claims_t2d'
    S3_FORESITE_ENROLLMENT_OUT_TEMPLATE = S3_FORESITE_OUT + '{}/enrollment_t2d'

    runner.run_spark_script('reload_codes.sql', [
        ['analyticsdb_schema', ANALYTICSDB_SCHEMA, False]
    ])

    runner.run_spark_script('create_pharmacy_extract.sql', [
        ['analyticsdb_schema', ANALYTICSDB_SCHEMA, False],
        ['delivery_date', date]
    ])

    runner.run_spark_script('create_enrollment_extract.sql', [
        ['analyticsdb_schema', ANALYTICSDB_SCHEMA, False],
        ['delivery_date', date]
    ])

    extractor.export_table(
        runner.sqlContext, 'pharmacy_claims_t2d', ANALYTICSDB_SCHEMA,
        S3_FORESITE_PHARMACY_OUT_TEMPLATE.format(date.replace('-', ''))
    )

    extractor.export_table(
        runner.sqlContext, 'enrollment_t2d', ANALYTICSDB_SCHEMA,
        S3_FORESITE_ENROLLMENT_OUT_TEMPLATE.format(date.replace('-', ''))
    )


def main(args):
    # init
    spark, sqlContext = init("Foresite Capital Delivery")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
