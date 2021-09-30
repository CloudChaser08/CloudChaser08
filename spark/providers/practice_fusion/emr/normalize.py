import argparse
import os
import subprocess
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.emr.encounter import schema_v8 as encounter_schema
from spark.common.emr.diagnosis import schema_v8 as diagnosis_schema
from spark.common.emr.procedure import schema_v10 as procedure_schema
from spark.common.emr.lab_test import schema_v1 as lab_test_schema
from spark.common.emr.medication import schema_v9 as medication_schema
from spark.common.emr.clinical_observation import schema_v9 as clinical_observation_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.providers.practice_fusion.emr.transactional_schemas as transactional_schemas
import spark.providers.practice_fusion.emr.transactional_schemas_v1 as transactional_schemas_v1
import spark.helpers.postprocessor as pp
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger, get_spark_time


FEED_ID = '136'

MODEL_SCHEMA = {
    'clinical_observation': clinical_observation_schema,
    'diagnosis': diagnosis_schema,
    'encounter': encounter_schema,
    'lab_test': lab_test_schema,
    'medication': medication_schema,
    'procedure': procedure_schema
}
MODELS = ['procedure', 'clinical_observation', 'encounter', 'lab_test', 'medication', 'diagnosis']
OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/spark-output-3/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/opp_1186_warehouse/parquet/emr/2019-04-17/'

transaction_paths = []
matching_paths = []
hadoop_times = []
spark_times = []

NEW_LAYOUT_DATE = '2020-10-01'


def run(spark, runner, date_input, model=None, custom_input_path=None, custom_matching_path=None,
        test=False, end_to_end_test=False):

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/out/{}/'.format(
            date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/payload/{}/'.format(
            date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/emr/practice_fusion/{}/'.format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/matching/payload/emr/practice_fusion/{}/'.format(date_input.replace('-', '/'))

    if custom_input_path:
        input_path = custom_input_path

    if custom_matching_path:
        matching_path = custom_matching_path

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

        logger.log('Loading external table: gen_ref_whtlst')
        table_name = 'gen_ref_whtlst'
        external_table_loader.load_analytics_db_table(runner.sqlContext, 'dw', table_name, table_name)
        spark.table(table_name).cache_and_track(table_name).createOrReplaceTempView(table_name)
        spark.table(table_name).count()
    else:
        pass

    # New layout after 2020-10-01 (LABORDER supplied as LAB_RESULT name)
    has_template_v1 = \
        datetime.strptime(date_input, '%Y-%m-%d').date() >= datetime.strptime(NEW_LAYOUT_DATE, '%Y-%m-%d').date()
    if has_template_v1:
        source_table_schemas = transactional_schemas_v1
    else:
        source_table_schemas = transactional_schemas

    records_loader.load_and_clean_all_v2(runner, input_path, source_table_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, ['claimId', 'patientId', 'hvJoinKey'], table_name='matching_payload')

    matching_payload_df = spark.table('matching_payload')
    cleaned_matching_payload_df = (
        pp.compose(pp.trimmify, pp.nullify)(matching_payload_df))
    cleaned_matching_payload_df.cache_and_track('matching_payload').createOrReplaceTempView('matching_payload')

    logger.log('Apply custom nullify trimmify')
    for table in source_table_schemas.TABLE_CONF:
        pp.nullify(
            pp.trimmify(spark.table(table))
            , ['NULL', 'Null', 'null', 'unknown', 'Unknown', 'UNKNOWN', '19000101', ''])\
            .cache_and_track(table).createOrReplaceTempView(table)

    if has_template_v1:
        spark.table('lab_result').cache_and_track('laborder').createOrReplaceTempView('laborder')

    earliest_service_date = pp.get_gen_ref_date(spark, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE', get_as_string=True)
    available_start_date = pp.get_gen_ref_date(spark, FEED_ID, 'HVM_AVAILABLE_HISTORY_START_DATE', get_as_string=True)
    earliest_diagnosis_date = pp.get_gen_ref_date(spark, FEED_ID, 'EARLIEST_VALID_DIAGNOSIS_DATE', get_as_string=True)

    variables = [['VDR_FILE_DT', str(date_input), False],
                 ['AVAILABLE_START_DATE', available_start_date, False],
                 ['EARLIEST_SERVICE_DATE', earliest_service_date, False],
                 ['EARLIEST_DIAGNOSIS_DATE', earliest_diagnosis_date, False]]

    models = [model] if model else MODELS
    for mdl in models:
        sql_file_path = os.path.dirname(script_path) + '/' + mdl + '/'
        if test:
            normalized_output = runner.run_all_spark_scripts(variables, directory_path=sql_file_path)
            df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                              columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])
            df.collect()
        else:
            if mdl == 'lab_test':
                logger.log('Custom chunk processing for {}'.format(mdl))
                spark.table('laborder').cache_and_track('laborder_full').createOrReplaceTempView('laborder_full')
                for i in range(10):
                    key = str(i)
                    logger.log('Custom chunk={} processing for {}'.format(key, mdl))
                    v_sql = """SELECT a.*, right(nvl(laborder_id,'0'), 1) as laborder_id_key FROM laborder_full a 
                    WHERE right(nvl(laborder_id,'0'), 1) = '{}' """.format(key)
                    this_df = spark.sql(v_sql).repartition(int(
                        spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'laborder_id')
                    this_df = this_df.cache_and_track('laborder')
                    this_df.createOrReplaceTempView('laborder')

                    normalized_output = runner.run_all_spark_scripts(variables, directory_path=sql_file_path)
                    df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                                      columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])
                    unload_file_cnt = 100
                    _columns = df.columns
                    _columns.remove('part_hvm_vdr_feed_id')
                    _columns.remove('part_mth')

                    normalized_records_unloader.unload(
                        spark, runner, df, 'part_mth', date_input, FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
                        date_partition_name='part_mth', columns=_columns,  staging_subdir=mdl,
                        unload_partition_count=unload_file_cnt,  distribution_key='row_id', substr_date_part=False
                    )
            elif mdl == 'medication':
                logger.log('Custom chunk processing for {}'.format(mdl))
                spark.table('prescription').cache_and_track('prescription_full').createOrReplaceTempView('prescription_full')
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format('prescription'))
                v_sql = """SELECT a.*, right(nvl(prescription_id,'00'), 2) as vdr_medctn_ord_id_key
                    FROM prescription_full a  """
                spark.sql(v_sql).repartition(100).write.parquet('/stagingout/{}/'.format('prescription'), compression='gzip',
                                                                mode='append', partitionBy=['vdr_medctn_ord_id_key'])

                this_tbl1 = 'practice_fusion_emr_norm_pre_emr_medctn'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl1))
                this_tbl2 = 'practice_fusion_emr_norm_dedup_emr_medctn'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl2))

                for i in range(100):
                    key = str(i).zfill(2)
                    logger.log('Custom chunk={} processing for {}'.format(key, mdl))
                    this_df = spark.read.parquet('/stagingout/{}/vdr_medctn_ord_id_key={}/'.format('prescription', key))
                    this_df = this_df.cache_and_track('prescription')
                    this_df.createOrReplaceTempView('prescription')

                    runner.run_spark_script(
                        '0_practice_fusion_emr_norm_pre_emr_medctn.sql', variables,
                        source_file_path=sql_file_path, return_output=True).repartition(
                            int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_medctn_ord_id').\
                        write.parquet('/stagingout/{}/vdr_medctn_ord_id_key={}/'.format(this_tbl1, key)
                                      , compression='gzip', mode='append')
                    # 2
                    logger.log('Custom pre-final chunk={} processing for {}'.format(key, mdl))
                    # reassign practice_fusion_emr_norm_pre_emr_medctn table
                    logger.log('reassign table for {}'.format(this_tbl1))
                    this_df1 = spark.read.parquet(
                        '/stagingout/{}/vdr_medctn_ord_id_key={}/'.format(this_tbl1, key))
                    this_df1 = this_df1.cache_and_track(this_tbl1)
                    this_df1.createOrReplaceTempView(this_tbl1)

                    this_df2 = runner.run_spark_script('1_practice_fusion_emr_norm_dedup_emr_medctn.sql', variables,
                                                       source_file_path=sql_file_path, return_output=True).repartition(
                                                           int(spark.sparkContext.getConf().get(
                                                             'spark.sql.shuffle.partitions')), 'vdr_medctn_ord_id')
                    this_df2 = this_df2.cache_and_track(this_tbl2)
                    this_df2.createOrReplaceTempView(this_tbl2)

                    logger.log('normalized_records_unloader processing for {}'.format(mdl))
                    normalized_output = runner.run_spark_script('2_practice_fusion_emr_norm_emr_medctn.sql',
                                                                variables, source_file_path=sql_file_path
                                                                , return_output=True)
                    df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                                      columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])

                    unload_file_cnt = 100
                    _columns = df.columns
                    _columns.remove('part_hvm_vdr_feed_id')
                    _columns.remove('part_mth')

                    normalized_records_unloader.unload(
                        spark, runner, df, 'part_mth', date_input, FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
                        date_partition_name='part_mth', columns=_columns,  staging_subdir=mdl,
                        unload_partition_count=unload_file_cnt,  distribution_key='row_id', substr_date_part=False
                    )

            elif mdl == 'encounter':
                logger.log('Custom chunk processing for {}'.format(mdl))
                this_tbl1 = 'practice_fusion_emr_norm_emr_enc_1'
                logger.log('Custom chunk={} processing for {}'.format(this_tbl1, mdl))
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl1))
                runner.run_spark_script(
                    '0_practice_fusion_emr_norm_emr_enc_1.sql', variables,
                    source_file_path=sql_file_path, return_output=True).repartition(
                    int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_enc_id').\
                    write.parquet('/stagingout/{}/'.format(this_tbl1)
                                  , compression='gzip', mode='append', partitionBy=['vdr_enc_id_key'])

                this_tbl2 = 'practice_fusion_emr_norm_emr_enc_2'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl2))
                spark.table('appointment').cache_and_track('appointment_full').createOrReplaceTempView('appointment_full')
                for i in range(10):
                    key = str(i)
                    logger.log('Custom chunk={} processing for {}'.format(key, mdl))
                    v_sql = """SELECT a.* FROM appointment_full a
                    WHERE right(nvl(appointment_id,'0'), 1) = '{}' """.format(key)
                    this_df = spark.sql(v_sql).repartition(int(
                        spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'appointment_id')
                    this_df = this_df.cache_and_track('appointment')
                    this_df.createOrReplaceTempView('appointment')
                    runner.run_spark_script(
                        '1_practice_fusion_emr_norm_emr_enc_2.sql', variables,
                        source_file_path=sql_file_path, return_output=True).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_enc_id').\
                        write.parquet('/stagingout/{}/'.format(this_tbl2)
                                      , compression='gzip', mode='append', partitionBy=['vdr_enc_id_key'])

                this_tbl3 = 'practice_fusion_emr_norm_emr_enc_pre_final'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl3))
                for i in range(10):
                    key = str(i)
                    logger.log('Custom pre-final chunk={} processing for {}'.format(key, mdl))

                    # reassign practice_fusion_emr_norm_emr_enc_1 table
                    logger.log('reassign table for {}'.format(this_tbl1))
                    this_df1 = spark.read.parquet(
                        '/stagingout/{}/vdr_enc_id_key={}/'.format(this_tbl1, key)).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_enc_id')
                    this_df1 = this_df1.cache_and_track(this_tbl1)
                    this_df1.createOrReplaceTempView(this_tbl1)

                    # reassign practice_fusion_emr_norm_emr_enc_2 table
                    logger.log('reassign table for {}'.format(this_tbl2))
                    this_df2 = spark.read.parquet(
                        '/stagingout/{}/vdr_enc_id_key={}/'.format(this_tbl2, key)).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_enc_id')
                    this_df2 = this_df2.cache_and_track(this_tbl2)
                    this_df2.createOrReplaceTempView(this_tbl2)

                    this_df3 = runner.run_spark_script('2_practice_fusion_emr_norm_emr_enc_pre_final.sql', variables,
                                                       source_file_path=sql_file_path, return_output=True).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_enc_id')
                    this_df3 = this_df3.cache_and_track(this_tbl3)
                    this_df3.createOrReplaceTempView(this_tbl3)

                    logger.log('normalized_records_unloader processing for {}'.format(mdl))
                    normalized_output = runner.run_spark_script('3_practice_fusion_emr_norm_emr_enc_final.sql',
                                                                variables, source_file_path=sql_file_path
                                                                , return_output=True)
                    df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                                      columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])

                    unload_file_cnt = 100
                    _columns = df.columns
                    _columns.remove('part_hvm_vdr_feed_id')
                    _columns.remove('part_mth')

                    normalized_records_unloader.unload(
                        spark, runner, df, 'part_mth', date_input, FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
                        date_partition_name='part_mth', columns=_columns,  staging_subdir=mdl,
                        unload_partition_count=unload_file_cnt,  distribution_key='row_id', substr_date_part=False
                    )
            elif mdl == 'diagnosis':
                logger.log('Custom chunk processing for {}'.format(mdl))
                this_tbl1 = 'practice_fusion_emr_norm_emr_pre_diag'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl1))
                spark.table('diagnosis').cache_and_track('diagnosis_full').createOrReplaceTempView('diagnosis_full')
                for i in range(10):
                    key = str(i)
                    logger.log('Custom chunk={} processing for {}'.format(key, mdl))
                    v_sql = """SELECT a.* FROM diagnosis_full a
                    WHERE right(nvl(diagnosis_id,'0'), 1) = '{}' """.format(key)
                    this_df = spark.sql(v_sql).repartition(int(
                        spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'diagnosis_id')
                    this_df = this_df.cache_and_track('diagnosis')
                    this_df.createOrReplaceTempView('diagnosis')
                    runner.run_spark_script(
                        '0_practice_fusion_emr_norm_emr_pre_diag.sql', variables,
                        source_file_path=sql_file_path, return_output=True).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_diag_id'). \
                        write.parquet('/stagingout/{}/vdr_diag_id_key={}/'.format(this_tbl1, key)
                                      , compression='gzip', mode='append')

                this_tbl2 = 'practice_fusion_emr_norm_emr_pre_diag_2'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl2))
                spark.table('encounter').cache_and_track('encounter_full').createOrReplaceTempView('encounter_full')
                for i in range(10):
                    key = str(i)
                    logger.log('Custom chunk={} processing for {}'.format(key, mdl))
                    v_sql = """SELECT a.* FROM encounter_full a
                    WHERE right(nvl(encounter_id,'0'), 1) = '{}' """.format(key)
                    this_df2 = spark.sql(v_sql).repartition(int(
                        spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'encounter_id')
                    this_df2 = this_df2.cache_and_track('encounter')
                    this_df2.createOrReplaceTempView('encounter')
                    runner.run_spark_script(
                        '1_practice_fusion_emr_norm_emr_pre_diag_2.sql', variables,
                        source_file_path=sql_file_path, return_output=True).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_diag_id'). \
                        write.parquet('/stagingout/{}/vdr_diag_id_key={}/'.format(this_tbl2, key)
                                      , compression='gzip', mode='append')

                this_tbl3 = 'practice_fusion_emr_norm_emr_dedup_diag'
                hdfs_utils.clean_up_output_hdfs('/stagingout/{}/'.format(this_tbl3))
                for i in range(10):
                    key = str(i)
                    logger.log('Custom dedup chunk={} processing for {}'.format(key, mdl))

                    # reassign practice_fusion_emr_norm_emr_pre_diag table
                    logger.log('reassign table for {}'.format(this_tbl1))
                    this_df1 = spark.read.parquet(
                        '/stagingout/{}/vdr_diag_id_key={}/'.format(this_tbl1, key)).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_diag_id')
                    this_df1 = this_df1.cache_and_track(this_tbl1)
                    this_df1.createOrReplaceTempView(this_tbl1)

                    # reassign practice_fusion_emr_norm_emr_pre_diag_2 table
                    logger.log('reassign table for {}'.format(this_tbl2))
                    this_df2 = spark.read.parquet(
                        '/stagingout/{}/vdr_diag_id_key={}/'.format(this_tbl2, key)).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_diag_id')
                    this_df2 = this_df2.cache_and_track(this_tbl2)
                    this_df2.createOrReplaceTempView(this_tbl2)

                    this_df3 = runner.run_spark_script('2_practice_fusion_emr_norm_emr_dedup_diag.sql', variables,
                                                       source_file_path=sql_file_path, return_output=True).repartition(
                        int(spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'vdr_diag_id')
                    this_df3 = this_df3.cache_and_track(this_tbl3)
                    this_df3.createOrReplaceTempView(this_tbl3)

                    logger.log('normalized_records_unloader processing for {}'.format(mdl))
                    normalized_output = runner.run_spark_script('3_practice_fusion_emr_norm_emr_diag.sql',
                                                                variables, source_file_path=sql_file_path
                                                                , return_output=True)
                    df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                                      columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])

                    unload_file_cnt = 100
                    _columns = df.columns
                    _columns.remove('part_hvm_vdr_feed_id')
                    _columns.remove('part_mth')

                    normalized_records_unloader.unload(
                        spark, runner, df, 'part_mth', date_input, FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
                        date_partition_name='part_mth', columns=_columns,  staging_subdir=mdl,
                        unload_partition_count=unload_file_cnt,  distribution_key='row_id', substr_date_part=False
                    )
            else:
                logger.log('Processing for {}'.format(mdl))
                normalized_output = runner.run_all_spark_scripts(variables, directory_path=sql_file_path)
                df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                                  columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])
                unload_file_cnt = 100
                if mdl == 'procedure':
                    unload_file_cnt = 40
                _columns = df.columns
                _columns.remove('part_hvm_vdr_feed_id')
                _columns.remove('part_mth')

                normalized_records_unloader.unload(
                    spark, runner, df, 'part_mth', date_input, FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
                    date_partition_name='part_mth', columns=_columns,  staging_subdir=mdl,
                    unload_partition_count=unload_file_cnt,  distribution_key='row_id', substr_date_part=False
                )

    if not test and not end_to_end_test:
        transaction_paths.append(input_path)
        matching_paths.append(matching_path)


def main(args):
    models = MODELS

    if args.models:
        models = args.models.split(',')

    if args.end_to_end_test:
        output_path = OUTPUT_PATH_TEST
    elif args.output_path:
        output_path = args.output_path
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    # # init 52428800
    conf_parameters = {
        'spark.default.parallelism': 4000,
        'spark.sql.shuffle.partitions': 4000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 26214400,
        'spark.shuffle.sasl.timeout': 60000,
        'spark.task.maxFailures': 8,
        'spark.max.executor.failures': 800,
        'spark.sql.broadcastTimeout': '36000'
    }

    for model in models:
        spark, sql_context = init('Practice Fusion {} Normalization'.format(model), conf_parameters=conf_parameters)
        runner = Runner(sql_context)

        run(spark, runner, args.date, model, custom_input_path=args.input_path,
            custom_matching_path=args.matching_path, end_to_end_test=args.end_to_end_test)
        spark.stop()

        if not args.end_to_end_test:
            spark_times.append(get_spark_time())

        # the full data set is reprocessed every time
        backup_path = output_path.replace('salusv', 'salusv/backup')
        subprocess.check_output(['aws', 's3', 'rm', '--recursive', backup_path + model])
        subprocess.check_call(['aws', 's3', 'mv', '--recursive', output_path + model, backup_path + model])

        if args.end_to_end_test:
            normalized_records_unloader.distcp(output_path)
        else:
            hadoop_times.append(normalized_records_unloader.timed_distcp(output_path))

    if not args.end_to_end_test:
        total_hadoop_time = sum(hadoop_times)
        total_spark_time = sum(spark_times)

        combined_trans_paths = ','.join(transaction_paths)
        combined_matching_paths = ','.join(matching_paths)

        logger.log_run_details(
            provider_name='Practice Fusion',
            data_type=DataType.EMR,
            data_source_transaction_path=combined_trans_paths,
            data_source_matching_path=combined_matching_paths,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=args.date
        )

        RunRecorder().record_run_details(total_spark_time, total_hadoop_time)
    logger.log('All Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--input_path', help='Overwrite default input path with this value')
    parser.add_argument('--matching_path', help='Overwrite default matching path with this value')
    parser.add_argument('--output_path', help='Overwrite default output path with this value')
    parser.add_argument('--models', help='Comma-separated list of models to normalize instead of all models')
    args = parser.parse_args()
    main(args)
