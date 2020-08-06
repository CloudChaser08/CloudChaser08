import argparse
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import spark.common.utility.logger as logger
from spark.runner import Runner
from spark.spark_setup import init
import spark.datamart.covid19.context as context
import spark.datamart.datamart_util as dmutil
from spark.datamart.covid19.labtests.builder import Covid19LabBuilder
from spark.datamart.covid19.labtests.transformer import Covid19LabTransformer
from spark.datamart.covid19.labtests.publisher import Covid19LabPublisher
script_path = __file__

UTC_NOW = datetime.utcnow()
ARCHIVE_DIR = str(UTC_NOW.strftime('%Y%m%d%H%M%S'))

_asset_typ = context.DATAMART_NAME
_data_typ = context.DATA_TYP
_load_ind  = context.REFRESH_IND
_datamart_name = context.DATAMART_NAME
_datamart_desc = context.DATAMART_SHORT_NOTES

_first_run_month = context.LAB_FIRST_RUN_MONTH
_refresh_nbr_of_months = context.LAB_REFRESH_NBR_OF_MONTHS

_production = context.PRODUCTION
_transform = context.TRANSFORM
_datamart_path = context.LAB_DATAMART_PATH
_datamart_stage_path = context.LAB_DATAMART_STAGE_PATH
_datamart_archive_path = context.LAB_DATAMART_ARCHIVE_PATH

"""
Package will build COVID19 Datamart. Sourced from LabTests providers and Covid19 Result Table.
This normalization routine run on daily and refresh most recent 6 months.
Before s3-refresh(transform) existing dataset will be moved into archive location.
Final Output S3 Location: s3a://salusv/warehouse/datamart/covid19/lab/
First Start Part Month is 2018-01
Partitioned table's number of parquet files are pre-configured and compressed by gzip
This module will create production external table (if not exists)
publish covid datamart dataset status (v_mdata)
"""


def build(spark, runner, requested_list_of_months, test=False):
    """
    :param spark:
    :param runner:
    :param requested_list_of_months: input from parameter
    :param test:
    :return:
    """

    """
    a. Use predefined 
    b. Extract Input Source 
    c. Build Covid Fact, Reference, Snapshot and Summary
    d. Transform into Local HDFS
    """
    logger.log(' -build: started')

    covid19LabBuilder = Covid19LabBuilder(
            spark
            , runner
            , requested_list_of_months
            , test
        )

    covid19LabBuilder.build_all_tests()
    covid19LabBuilder.build_covid_tests()
    covid19LabBuilder.build_covid_tests_cleansed()
    covid19LabBuilder.build_covid_ref()
    covid19LabBuilder.build_covid_snapshot()
    covid19LabBuilder.build_covid_sum()

    logger.log(' -build: completed')


def transform(requested_list_of_months, output_to_transform_path, archive_dir, test=False):
    """
    :param requested_list_of_months: input from parameter
    :param output_to_transform_path: Output to transform path or production
    :param test:
    :return:
    """

    """
    a. clean-up S3 Stage Location
    b. transform data from HDFS to S3-Stage  [HDFS -> S3 STAGE]
    c. [For NON-Production ] transform current prod 
            data from S3-Prod to S3-Archive [S3 PROD -> S3 ARCHIVE]
    d. Clean-up S3 Prod and transform from S3-Stage to S3-Prod [S3 STAGE -> S3 PROD]
    """

    logger.log(' -transform: started')

    OUTPUT_BASE = _production
    if output_to_transform_path:
        OUTPUT_BASE = _transform

    OUTPUT_DATAMART_BASE = '{}{}'.format(OUTPUT_BASE, _datamart_path)
    OUTPUT_DATAMART_STAGE_BASE = '{}{}'.format(OUTPUT_BASE, _datamart_stage_path)
    OUTPUT_DATAMART_ARCHIVE_BASE = '{}{}'.format(OUTPUT_BASE, _datamart_archive_path)

    covid19LabTransformer = Covid19LabTransformer(
            requested_list_of_months
            , OUTPUT_DATAMART_BASE
            , OUTPUT_DATAMART_STAGE_BASE
            , OUTPUT_DATAMART_ARCHIVE_BASE
            , archive_dir
            , test
        )

    covid19LabTransformer.cleanup_stage_if_exists()
    covid19LabTransformer.trans_local_to_s3stage()

    if not context.LAB_SKIP_ARCHIVE:
        covid19LabTransformer.archive_current_prod()
    else:
        logger.log('    -transform: Alert: Current Production Archive process skipped')

    covid19LabTransformer.move_stage_to_prod()

    logger.log(' -transform: completed')


def publish(refresh_time_id, requested_list_of_months, output_to_transform_path, conf_parameters, test=False):
    """
    :param refresh_time_id:  Process started Time ID (yyyymmddHHMMss)
    :param requested_list_of_months: requested number of months
    :param output_to_transform_path: is this output to transform path?
    :param test:
    :return:
    """

    """
        Publish Covid Tables and Log Tables
            a. Create External Tables (if not exist)
            b. Apply MSCK repair and refresh Tables
            c. Update Status Log Tables
                a. Create Log Table and View
                b. Insert Log Information
                c. Refresh view and Table
    """
    logger.log(' -publish: started')

    covid19LabPublisher = Covid19LabPublisher(
            refresh_time_id
            , requested_list_of_months
            , output_to_transform_path
            , _asset_typ
            , _data_typ
            , _load_ind
            , _datamart_desc
            , test
            )

    if not output_to_transform_path:
        logger.log('Initialize the spark context-Publish')
        spark, sqlContext = init("{} Publish Covid Tables".format(
            _datamart_name), conf_parameters=conf_parameters)

        # initialize runner
        runner = Runner(sqlContext)

        covid19LabPublisher.create_table_if_not_and_repair(spark, runner)

        covid19LabPublisher.update_mdata(spark, runner)
    else:
        logger.log(' -publish:  Update Log and Repair Covid table- steps are skipped for transform location')

    logger.log(' -publish: completed')


def main(args):
    """
    resolve arguments
        construct target location and refresh time-period
    """
    end_month = args.month[:7] # YYYY-MM
    nbr_of_hist_months = int(args.nbr_of_hist_months) if args.nbr_of_hist_months else _refresh_nbr_of_months

    # YYYY-MM
    start_month = args.start_month if args.start_month \
        else (datetime.strptime(end_month, '%Y-%m') - relativedelta(months=nbr_of_hist_months)).strftime('%Y-%m')

    if args.first_run:
        logger.log('[{}] Requested to start from first month {}'.format(_datamart_name, _first_run_month))
        start_month = _first_run_month

    output_to_transform_path = False
    if args.output_to_transform_path:
        logger.log('[{}] Requested to load into TRANSFORMED location {}'.format(_datamart_name, _transform))
        output_to_transform_path = True
    else:
        logger.log('[{}] Requested to load into PRODUCTION location {}'.format(_datamart_name, _production))

    # ENABLE THIS ONCE TEST CASES ADDED
    end_to_end_test = False
    if args.end_to_end_test:
        logger.log('[{}] Requested to run test'.format(_datamart_name))
        end_to_end_test = True

    # ------------------------------------------------------------------------------------------------
    requested_list_of_months = []
    if start_month != end_month:
        logger.log('[{}] Requested to refresh months between {} and {}'.format(_datamart_name, start_month, end_month))
        requested_list_of_months = dmutil.get_list_of_months_v1(start_month, end_month)
    else:
        logger.log('[{}] Requested to refresh month = {}'.format(_datamart_name, start_month))
        requested_list_of_months.append(start_month)

    logger.log('[{}] Requested List of Months {}'.format(_datamart_name, requested_list_of_months))

    this_archive_dir = ARCHIVE_DIR

    # init
    conf_parameters = {
        'spark.default.parallelism': 5000,
        'spark.sql.shuffle.partitions': 5000,
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.files.maxPartitionBytes': 3221225472,
        'spark.reducer.maxBlocksInFlightPerAddress': 64,
        'spark.reducer.maxReqsInFlight': 192,
        'spark.shuffle.service.enabled': 'true',
        'spark.shuffle.sasl.timeout': 60000,
        'spark.network.timeout': '480s',
        'spark.port.maxRetries': 64,
        'spark.yarn.maxAppAttempts': 1,
        'spark.task.maxFailures': 8,
        'spark.max.executor.failures': 800
    }
    # ================================================================================================

    # ------------------------------------------------------------------------------------------------
    # build Covid
    #   - collect source data and load into HDFS
    #   - integrate with covid business rules
    #   - construct covid fact and summary
    # ------------------------------------------------------------------------------------------------

    logger.log('Initialize the spark context')
    spark, sqlContext = init("{} Refresh {}-{}".format(
        _datamart_name, start_month, end_month), conf_parameters=conf_parameters)

    # -- initialize runner
    runner = Runner(sqlContext)

    build(spark, runner, requested_list_of_months, end_to_end_test)

    logger.log('Stopping the spark context')
    spark.stop()

    # ================================================================================================

    # ------------------------------------------------------------------------------------------------
    # transfer data:
    #   - transfer data from hdfs to s3-stage
    #   - copy all data from s3-prod to s3-archive (if requested)
    #   - remove requested months data from s3-prod
    #   - move s3-stage into s3-prod
    #   done.
    # ------------------------------------------------------------------------------------------------

    transform(requested_list_of_months, output_to_transform_path, this_archive_dir, end_to_end_test)

    # ================================================================================================

    # ------------------------------------------------------------------------------------------------
    # publish
    #   - if prod environment,
    #           create external table if not there
    #           repair external table
    #           update refresh status
    #   - notify data ops
    # ------------------------------------------------------------------------------------------------
    refresh_time_id = str((datetime.utcnow() - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S"))
    publish(refresh_time_id, requested_list_of_months, output_to_transform_path, conf_parameters, end_to_end_test)

    # ================================================================================================


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', type=str, help='refresh recent month (format yyyy-mm)')
    parser.add_argument('--start_month', type=str, help='refresh start month (format yyyy-mm)')
    parser.add_argument('--nbr_of_hist_months', type=str
                        , help='number of months to refresh in history (exclude current)')

    parser.add_argument('--first_run', default=False, action='store_true',
                        help='enable this flag to refresh start from 2018-01')

    parser.add_argument('--output_to_transform_path', default=False, action='store_true',
                        help='transform loc: skipped steps are, archive and external table create')
    parser.add_argument('--end_to_end_test', default=False, action='store_true',
                        help='run end to end test')
    args = parser.parse_args()
    main(args)

