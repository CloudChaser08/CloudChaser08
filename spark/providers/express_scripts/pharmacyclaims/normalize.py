import os
import argparse
import subprocess
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility import logger
import spark.helpers.file_utils as file_utils
import spark.helpers.records_loader as records_loader
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas as pharma_schemas
import spark.providers.express_scripts.pharmacyclaims.transactional_schemas as source_table_schemas
import pyspark.sql.functions as f

ACCREDO_PREFIX = '10130X001_HV_ODS_Claims'
NON_ACCREDO_PREFIX = '10130X001_HV_RX_Claims'

STAGE_REVERSE_TRANS_PATH = '/temp_stage/'
REVERSAL_APPLY_HIST_MONTHS = 2


def run(date_input, first_run, reversal_apply_hist_months, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log("esi-rx: this normalization apply rejects/reversal process "
               "and re-write output (warehouse) data for "
               "most recent <REVERSAL_APPLY_HIST_MONTHS> months + current month. ")
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'esi'
    schema = pharma_schemas['schema_v4']
    output_table_names_to_schemas = {
        'esi_06_norm_final': schema
    }
    provider_partition_name = 'express_scripts'

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        vdr_feed_id=16,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    script_path = __file__
    normalized_path = ""
    input_path_prev = ""
    if test:
        input_path_prev = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/input_prev/'
        ) + '/'
        driver.input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/input/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/matching/'
        ) + '/'
        normalized_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/express_scripts/pharmacyclaims/resources/normalized/'
        ) + '/'

    driver.load(extra_payload_cols=['RXNumber', 'hvJoinKey'])

    # ----------------------------------------------------------------------------------------
    # load raw source and parquet source for [recent 2 months + current month]
    # ----------------------------------------------------------------------------------------
    logger.log('-loading: external tables')
    # reversal identification columns
    sql_stmnt = "select date_of_service, pharmacy_claim_id, pharmacy_claim_ref_id from {}"
    normalized_output_provider_path = os.path.join(driver.output_path, schema.output_directory,
                                                   '{}={}/'.format(schema.provider_partition_column
                                                                   , provider_partition_name))
    # collect list of days /months for reject and reversals
    start_date = (datetime.strptime(date_input, '%Y-%m-%d')
                  - relativedelta(months=reversal_apply_hist_months)).replace(day=1)  # start date
    delta = datetime.strptime(date_input, '%Y-%m-%d') - start_date       # as timedelta
    conf = driver.source_table_schema.TABLE_CONF['transaction']
    list_of_months = {}
    for i in range(delta.days + 1):
        d_path = (start_date + timedelta(days=i)).strftime('%Y-%m')
        list_of_months[d_path.replace('-', '')] = d_path
    logger.log('-loading: list of months {}'.format(list_of_months.values()))

    if test:
        logger.log('    -loading: prev transaction files from {}'.format(input_path_prev))
        records_loader.load(driver.runner, input_path_prev, source_table_conf=conf, load_file_name=True
                            , file_name_col='input_file_name', spark_context=driver.spark
                            ).createOrReplaceTempView('_temp_rev_transaction')
        driver.spark.read.csv(
            normalized_path, sep="|", quote='"', header=True).createOrReplaceTempView('_temp_pharmacyclaims_nb')
    else:
        if first_run:
            logger.log('-loading: requested first run and not required to apply reversals')
            driver.spark.sql(
                sql_stmnt.format("transaction where 1 = 2")).createOrReplaceTempView('_temp_rev_transaction')
            driver.spark.sql("select * from transaction where 1=2").createOrReplaceTempView('_temp_pharmacyclaims_nb')
        else:
            # ----------load incoming data to identify rejects and reversals----------
            # load reject reversal source form raw salusv/incoming
            #     key columns does not exist in warehouse / normalized output model
            #     interested 2 columns: pharmacy_claim_id, pharmacy_claim_ref_id
            has_data = False
            # collect incoming data(3 columns) and save into local
            file_utils.clean_up_output_hdfs(STAGE_REVERSE_TRANS_PATH)
            S3_EXPRESS_SCRIPTS_IN = "s3://salusv/incoming/{}/{}/".format(driver.data_type, provider_name)
            for i in range(delta.days + 1):
                s3_esi_path_in = S3_EXPRESS_SCRIPTS_IN + (start_date + timedelta(days=i)).strftime('%Y/%m/%d')
                try:
                    subprocess.check_call(['aws', 's3', 'ls', s3_esi_path_in])
                    has_data = True
                except:
                    continue

                # load into /temp_staging hdfs location
                logger.log('    -loading: prev transaction files from {}'.format(s3_esi_path_in + '/'))
                records_loader.load(driver.runner, s3_esi_path_in + '/', source_table_conf=conf, load_file_name=True
                                    , file_name_col='input_file_name', spark_context=driver.spark
                                    ).createOrReplaceTempView('_temp_esi_rx_src')
                driver.spark.sql(sql_stmnt.format('_temp_esi_rx_src')).repartition(5).write.parquet(
                    STAGE_REVERSE_TRANS_PATH, compression='gzip', mode='append')

            # final temp table
            if has_data:
                logger.log('    -loading: '
                           'create external table {} from {}'.format('_temp_rev_transaction', STAGE_REVERSE_TRANS_PATH))
                driver.spark.read.parquet(STAGE_REVERSE_TRANS_PATH).createOrReplaceTempView('_temp_rev_transaction')
            else:
                logger.log('    -loading: there is no transaction data for reversal apply. '
                           'create empty external table {} from '
                           'existing transaction table'.format('_temp_rev_transaction'))
                driver.spark.sql(
                    sql_stmnt.format("transaction where 1 = 2")).createOrReplaceTempView('_temp_rev_transaction')

            # ----------load parquet from warehouse to apply rejects and reversals----------
            logger.log('    -loading:  create external table {} from warehouse '
                       'normalized data path {}'.format('_temp_pharmacyclaims_nb', STAGE_REVERSE_TRANS_PATH))

            driver.spark.read.parquet(normalized_output_provider_path) \
                .filter((f.col('part_best_date') >= '{}'.format(min(list_of_months.values())))
                        & (f.col('part_best_date') <= '{}'.format(max(list_of_months.values())))) \
                .createOrReplaceTempView('_temp_pharmacyclaims_nb')

            # ----------------------------------------------------------------------------------------

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.stop_spark()

    if not end_to_end_test and not test:
        logger.log_run_details(
            provider_name=provider_partition_name,
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=driver.input_path,
            data_source_matching_path=driver.matching_path,
            output_path=driver.output_path,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )

    if not test:
        if first_run:
            logger.log('first_run!! Historical Backup NOT required')
        else:
            # existing data will be moved into backup location
            logger.log('Backup historical data')
            if end_to_end_test:
                tmp_path = \
                    's3://salusv/testing/dewey/airflow/e2e/{}/pharmacyclaims/backup/'.format(provider_partition_name)
            else:
                tmp_path = 's3://salusv/backup/{}/pharmacyclaims/{}/{}={}/'.format(
                    provider_partition_name, date_input, schema.provider_partition_column, provider_partition_name)
            date_part = 'part_best_date={}/'

            for month in list_of_months.values():
                subprocess.check_call(
                    ['aws', 's3', 'mv', '--recursive', normalized_output_provider_path + date_part.format(month)
                        , tmp_path + date_part.format(month)]
                )

        driver.copy_to_output_path()
        if not first_run:
            file_utils.clean_up_output_hdfs(STAGE_REVERSE_TRANS_PATH)
    logger.log("Done")


if __name__ == '__main__':
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--first_run', default=False, action='store_true')
    parser.add_argument('--reversal_apply_nbr_of_hist_months', type=str)
    args = parser.parse_args()
    rev_apply_hist_months = int(args.reversal_apply_nbr_of_hist_months) \
        if args.reversal_apply_nbr_of_hist_months else REVERSAL_APPLY_HIST_MONTHS

    run(args.date, args.first_run, rev_apply_hist_months, args.end_to_end_test)
