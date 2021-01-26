import argparse
from datetime import datetime, timedelta
from spark.runner import Runner
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
import spark.helpers.extractor as extractor
from spark.spark_setup import init

script_path = __file__

S3_CELGENE_DEST_TEMPLATE = 's3://salusv/projects/celgene/hv000242/delivery/{}'


def run(spark, runner, date_input, test=False):

    if test:
        STAGING_DIR = '../../test/delivery/celgene_hv000242/resources/tmp/{}'
    else:
        STAGING_DIR = constants.hdfs_staging_dir

    PHARMACY_OUT_TEMPLATE = STAGING_DIR + '/pharmacy_claims'
    NPPES_OUT_TEMPLATE = STAGING_DIR + '/nppes'

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    start_date = (date_obj - timedelta(days=9)).isoformat()
    end_date = (date_obj - timedelta(days=3)).isoformat()

    runner.run_spark_script('create_pharmacy_extract.sql', [
        ['start_date', start_date],
        ['end_date', end_date]
    ], return_output=True).registerTempTable("pharmacyclaims_extract")

    runner.run_spark_script('create_nppes_extract.sql', return_output=True) \
          .registerTempTable("nppes_extract")

    if not test:
        extractor.export_table(
            runner.sqlContext, 'pharmacyclaims_extract', None,
            file_utils.get_abs_path(
                script_path, PHARMACY_OUT_TEMPLATE.format(date_input.replace('-', ''))
            ), partitions=1, output_file_name='pharmacy_claims_{}'.format(date_obj.strftime('%Y%m%d'))
        )

        extractor.export_table(
            runner.sqlContext, 'nppes_extract', None,
            file_utils.get_abs_path(
                script_path, NPPES_OUT_TEMPLATE.format(date_input.replace('-', ''))
            ), partitions=1, output_file_name='nppes_{}'.format(date_obj.strftime('%Y%m%d'))
        )


def main(args):
    # init
    spark, sql_context = init("Celgene Delivery")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date)

    spark.stop()

    normalized_records_unloader.distcp(S3_CELGENE_DEST_TEMPLATE.format(args.date.replace('-', '')))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
