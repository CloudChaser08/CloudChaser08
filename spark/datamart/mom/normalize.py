"""
 MOM Publish delivery
"""
import os
import argparse
import time
import subprocess
import inspect
import copy
from datetime import datetime, timedelta
from spark.runner import PACKAGE_PATH
import spark.common.utility.logger as logger
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.s3_constants as s3_constants
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.datamart.mom.publish_prep as P
import spark.datamart.mom.mom_util as mom_util
import spark.datamart.registry.registry_util as reg_util
from spark.common.marketplace_driver import MarketplaceDriver

from spark.common.datamart.mom.pharmacyclaims.pharmacyclaims_mom_model import schemas as mom_rx_schemas
from spark.common.datamart.mom.medicalclaims.medicalclaims_mom_model import schemas as mom_dx_schemas
from spark.common.datamart.mom.enrollmentrecords.enrollmentrecords_mom_model import schemas as mom_enr_schemas

from spark.common.datamart.registry.pharmacyclaims.pharmacyclaims_registry_model import schemas as reg_rx_schemas
from spark.common.datamart.registry.fact.registry_race_common_model import schemas as reg_race_schemas
from spark.common.datamart.registry.medicalclaims.medicalclaims_registry_model import schemas as reg_dx_schemas
from spark.common.datamart.registry.enrollmentrecords.enrollmentrecords_registry_model import schemas as reg_enr_schemas

UTC_NOW = datetime.utcnow()
ARCHIVE_TS = str(UTC_NOW.strftime('%Y%m%d%H%M%S'))

BIRTH_CUTOFF_START_DATE = '2017-01-01'
DEATH_CUTOFF_START_DATE = '2014-01-01'
DEATH_CUTOFF_SERVICE_DATE = '2015-10-01'

DATAMART_PATH = s3_constants.DATAMART_PATH
MOM_PATH = s3_constants.MOM_PATH

# ref_calendar_loc = 's3://salusv/reference/parquet/calendar/'
# ref_calendar = 'ref_calendar'

# Mom Staging/Warehouse Tables
_mom_cohort = '_mom_cohort'
_mom_masterset = '_mom_masterset'
_mom_medicalclaims_race = '_mom_medicalclaims_race'
_mom_medicalclaims = '_mom_medicalclaims'
_mom_enrollment = '_mom_enrollment'
_mom_pharmacyclaims = '_mom_pharmacyclaims'

# Mom Delivery/Warehouse Tables
publish_db = 'dw_mom'
publish_pat_list = '_publish_pat_list'
publish_masterset = '_publish_masterset'
publish_mom_pharmacyclaims = '_publish_mom_pharmacyclaims'
publish_mom_enrollment = '_publish_mom_enrollment'
publish_mom_medicalclaims = '_publish_mom_medicalclaims'

hvmom_masterset = 'hvmom_masterset'
hvmom_pharmacyclaims = 'hvmom_pharmacyclaims'
hvmom_enrollment = 'hvmom_enrollment'
hvmom_medicalclaims = 'hvmom_medicalclaims'

# HV Warehouse Tables
hvm_enrollment_loc = 's3://salusv/warehouse/parquet/enrollmentrecords/2017-03-22/part_provider=inovalon/'
hvm_enrollment = 'hvm_enrollment'

publish_dx_variables = [['_DX_', str(P._DX_), False],
                        ['_DX_DATE_RECEIVED_', str(P._DX_DATE_RECEIVED_), False],
                        ['_DX_DATE_SERVICE_', str(P._DX_DATE_SERVICE_), False],
                        ['_DX_DATE_SERVICE_END_', str(P._DX_DATE_SERVICE_END_), False],
                        ['_DX_INST_DATE_ADMITTED_', str(P._DX_INST_DATE_ADMITTED_), False],
                        ['_DX_INST_DATE_DISCHARGED_', str(P._DX_INST_DATE_DISCHARGED_), False]
                        ]

publish_rx_variables = [['_RX_', str(P._RX_), False],
                        ['_RX_DATE_WRITTEN_', str(P._RX_DATE_WRITTEN_), False],
                        ['_RX_DATE_SERVICE_', str(P._RX_DATE_SERVICE_), False],
                        ['_RX_DATE_AUTHORIZED_', str(P._RX_DATE_AUTHORIZED_), False],
                        ['_RX_DISCHARGE_DATE_', str(P._RX_DISCHARGE_DATE_), False],
                        ['_RX_OTHER_PAYER_DATE_', str(P._RX_OTHER_PAYER_DATE_), False]
                        ]

publish_enr_variables = [['_ENROLL_SOURCE_RECORD_DATE_', str(P._ENROLL_SOURCE_RECORD_DATE_), False],
                         ['_ENROLL_DATE_START_', str(P._ENROLL_DATE_START_), False],
                         ['_ENROLL_DATE_END_', str(P._ENROLL_DATE_END_), False]
                         ]

publish_shift_variables = [['_INDEX_PREG_', str(P._INDEX_PREG_), False],
                           ['_ADULT_DEATH_MONTH_', str(P._ADULT_DEATH_MONTH_), False],
                           ['_CHILD_DEATH_DATE_', str(P._CHILD_DEATH_DATE_), False],
                           ['_CHILD_DOB_', str(P._CHILD_DOB_), False]
                           ]

if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    date_input = args.date
    logger.log("Publish MOM- {}".format(date_input))

    # ------------------------ Provider specific configuration -----------------------
    logger.log('    -assign provider specific configuration')
    provider_name = 'inovalon'
    reg_rx_versioned_schema = reg_rx_schemas['schema_v1']
    reg_dx_versioned_schema = reg_dx_schemas['schema_v1']
    reg_enr_versioned_schema = reg_enr_schemas['schema_v1']
    reg_race_versioned_schema = reg_race_schemas['schema_v1']

    mom_rx_versioned_schema = mom_rx_schemas['schema_v1']
    mom_dx_versioned_schema = mom_dx_schemas['schema_v1']
    mom_enr_versioned_schema = mom_enr_schemas['schema_v1']

    output_table_names_to_schemas = {
        'publish_pharmacyclaims': mom_rx_versioned_schema,
        'publish_medicalclaims': mom_dx_versioned_schema,
        'publish_enrollment': mom_enr_versioned_schema
    }
    provider_partition_kv = 'part_provider={}/'.format(provider_name)

    mom_rx_path = os.path.join(MOM_PATH, mom_rx_versioned_schema.output_directory, provider_partition_kv)
    mom_dx_path = os.path.join(MOM_PATH, mom_dx_versioned_schema.output_directory, provider_partition_kv)
    mom_enr_path = os.path.join(MOM_PATH, mom_enr_versioned_schema.output_directory, provider_partition_kv)

    reg_rx_path = os.path.join(DATAMART_PATH, reg_rx_versioned_schema.output_directory, provider_partition_kv)
    reg_dx_path = os.path.join(DATAMART_PATH, reg_dx_versioned_schema.output_directory, provider_partition_kv)
    reg_enr_path = os.path.join(DATAMART_PATH, reg_enr_versioned_schema.output_directory, provider_partition_kv)
    reg_race_path = os.path.join(DATAMART_PATH, reg_race_versioned_schema.output_directory, provider_partition_kv)
    # ------------------------ Common for all providers -----------------------

    publish_variables = publish_rx_variables + publish_dx_variables + publish_enr_variables + publish_shift_variables
    variables = [['VDR_FILE_DT', str(date_input), False],
                 ['BIRTH_CUTOFF_START_DATE', str(BIRTH_CUTOFF_START_DATE), False],
                 ['DEATH_CUTOFF_START_DATE', str(DEATH_CUTOFF_START_DATE), False],
                 ['DEATH_CUTOFF_SERVICE_DATE', str(DEATH_CUTOFF_SERVICE_DATE), False]
                 ]

    directory_path = \
        os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename).replace(PACKAGE_PATH, "") + '/'

    # init
    spark, sql_context = init("Publish MOM- {}".format(date_input.replace('-', '')))

    # initialize runner
    runner = Runner(sql_context)

    logger.log('    <=====collect data=====>')
    # reference
    logger.log('    -load ref_calendar table')
    # spark.read.parquet(ref_calendar_loc).createOrReplaceTempView(ref_calendar)
    external_table_loader.load_analytics_db_table(
        sql_context, 'default', 'ref_calendar', 'ref_calendar'
    )

    # staging output
    staging_path = '/staging/created_dt={}/'.format(date_input)

    # collect cohort
    logger.log('    -load cohort table {}'.format(_mom_cohort))
    cohort_df = reg_util.get_mom_cohort(spark, provider_name)
    logger.log('        -{} table count {}'.format(_mom_cohort, cohort_df.count()))
    cohort_df.createOrReplaceTempView(_mom_cohort)

    logger.log('    -load _mom_masterset table {}'.format(_mom_masterset))
    masterset_df = reg_util.get_mom_masterset(spark, provider_name)
    logger.log('        -{} table count {}'.format(_mom_masterset, masterset_df.count()))
    masterset_df.createOrReplaceTempView(_mom_masterset)

    logger.log('    -load fact tables')
    for loc, tbl in [(reg_rx_path, _mom_pharmacyclaims), (reg_dx_path, _mom_medicalclaims),
                     (reg_enr_path, _mom_enrollment), (reg_race_path, _mom_medicalclaims_race),
                     (hvm_enrollment_loc, hvm_enrollment)]:
        logger.log('        -load {} table'.format(tbl))
        spark.read.parquet(loc).createOrReplaceTempView(tbl)

    logger.log('    <=====build new data=====>')
    all_tables_names = []
    qa_table_names = []
    logger.log('    -unload qa sqls')
    for mdl in ['race', 'birth', 'death']:
        logger.log('        -unload all qa sqls from {} module'.format(mdl))
        table_names = mom_util.sql_unload_and_read(
            runner, spark, 'qa', staging_path, directory_path + 'sql/{}/'.format(mdl), variables)
        qa_table_names = qa_table_names + table_names

    prod_table_names = []
    logger.log('    -unload prod sqls')
    for mdl in ['masterset']:
        logger.log('        -unload all prod sqls from {} module'.format(mdl))
        table_names = mom_util.sql_unload_and_read(
            runner, spark, 'prod', staging_path, directory_path + 'sql/{}/'.format(mdl), variables)
        prod_table_names = prod_table_names + table_names

    publish_table_names = []
    publish_fact_table_names = []
    logger.log('    -unload publish sqls')
    for mdl in ['publish_dim', 'publish_fact', 'publish_qa']:
        logger.log('        -unload all publish sqls from {} module'.format(mdl))
        if mdl == 'publish_fact':
            table_names = mom_util.sql_unload(
                runner, spark, 'deliverable', directory_path + 'sql/{}/'.format(mdl), variables + publish_variables)
            for table in table_names:
                unload_partition_count = 50 if table == '' else 10

                df = spark.table(table)
                schema_obj = output_table_names_to_schemas[table]

                logger.log('        -Saving data to local file system with schema {}'.format(schema_obj.name))
                output = MarketplaceDriver.apply_schema(df, schema_obj)
                _columns = output.columns
                _columns.remove(schema_obj.provider_partition_column)
                _columns.remove(schema_obj.date_partition_column)

                normalized_records_unloader.unload(
                    spark, runner, df,
                    schema_obj.date_partition_column,
                    str(date_input),
                    provider_name,
                    columns=_columns,
                    date_partition_name=schema_obj.date_partition_column,
                    provider_partition_name=schema_obj.provider_partition_column,
                    distribution_key=schema_obj.distribution_key,
                    staging_subdir=schema_obj.output_directory,
                    unload_partition_count=unload_partition_count
                )
                output.unpersist()
                publish_fact_table_names = publish_fact_table_names + table_names
        else:
            table_names = mom_util.sql_unload_and_read(
                runner, spark, 'deliverable', staging_path, directory_path + 'sql/{}/'.format(mdl),
                variables + publish_variables)
            publish_table_names = publish_table_names + table_names

    # final swap
    all_tables_names = all_tables_names + publish_fact_table_names + publish_table_names + \
                       prod_table_names + qa_table_names
    logger.log('.............done')

    logger.log('    -Stopping the spark context')
    spark.stop()

    # transfer to prod
    logger.log('    <=====transfer data=====>')

    mdl_list = list(set([mdl for mdl, tbl in all_tables_names]))
    for mdl in mdl_list:
        mt_list = [(m, t) for m, t in all_tables_names if m in [mdl]]
        for mdl, tbl in mt_list:
            logger.log('    -transfer to prod -> archive {} module'.format(mdl))
            curr_path = os.path.join(MOM_PATH, mdl, tbl + '/')
            arch_path = os.path.join(MOM_PATH, 'archive', 'ts='.format(ARCHIVE_TS), mdl, tbl + '/')
            logger.log('        from {}'.format(curr_path))
            logger.log('        ..to {}'.format(arch_path))
            while True:
                s3_file_cnt = s3_utils.get_s3_file_count(curr_path, True) or 0
                if s3_file_cnt > 0:
                    subprocess.check_call(
                        ['aws', 's3', 'mv', '--recursive', curr_path, arch_path]
                    )
                else:
                    logger.log('    -there is no files to move from {}'.format(curr_path))
                    break
        logger.log('.............archive done')

        logger.log('    -transfer to stage -> prod {} module'.format(mdl))
        stg_path = os.path.join(staging_path, mdl + '/')
        curr_path = os.path.join(MOM_PATH, mdl + '/')
        logger.log('        from {}'.format(stg_path))
        logger.log('        ..to {}'.format(curr_path))
        normalized_records_unloader.distcp(curr_path, src=stg_path)
        logger.log('.............deploy done')

    logger.log('    <=====refresh tables=====>')
    # init
    logger.log('    -re-init spark')
    spark, sql_context = init("Publish-Deploy MOM- {}".format(date_input.replace('-', '')))

    # initialize runner
    runner = Runner(sql_context)
    # ['publish_mom_pharmacyclaims', 'publish_mom_enrollment', 'publish_mom_medicalclaims']
    for tbl in publish_fact_table_names:
        spark.sql('msck repair table {db}._publish_{tbl}'.format(db=publish_db, tbl=tbl))
    logger.log('    -Stopping the spark context')
    spark.stop()
    logger.log('All Done')
