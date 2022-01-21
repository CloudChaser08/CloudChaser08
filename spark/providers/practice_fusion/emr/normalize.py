import os
import argparse
from datetime import datetime
import inspect
import copy
import pyspark.sql.functions as FN
from spark.runner import PACKAGE_PATH
from spark.common.marketplace_driver import MarketplaceDriver
import spark.helpers.postprocessor as pp
import spark.helpers.constants as constants
import spark.helpers.file_utils as file_utils
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.providers.practice_fusion.emr.transactional_schemas as transactional_schemas
import spark.providers.practice_fusion.emr.transactional_schemas_v1 as transactional_schemas_v1
from spark.common.utility import logger
from spark.common.emr.encounter import schemas as encounter_schema
from spark.common.emr.diagnosis import schemas as diagnosis_schema
from spark.common.emr.procedure import schemas as procedure_schema
from spark.common.emr.lab_test import schemas as lab_test_schema
from spark.common.emr.medication import schemas as medication_schema
from spark.common.emr.clinical_observation import schemas as clinical_observation_schema

LOAD_OUTPUT_OPP_DW = False
LOAD_INTO_TRANSFORM = True   # After full autmation, we can switch to prod
FEED_ID = '136'
FNULL = open(os.devnull, 'w')

MODEL_SCHEMA = {
    'clinical_observation': ('practice_fusion_emr_norm_emr_clin_obsn_final', clinical_observation_schema['schema_v11']),
    'diagnosis': ('practice_fusion_emr_norm_emr_diag', diagnosis_schema['schema_v10']),
    'encounter': ('practice_fusion_emr_norm_emr_enc_final', encounter_schema['schema_v10']),
    'lab_test': ('practice_fusion_emr_norm_emr_lab_test', lab_test_schema['schema_v3']),
    'medication': ('practice_fusion_emr_norm_emr_medctn', medication_schema['schema_v11']),
    'procedure': ('practice_fusion_emr_norm_emr_proc_final', procedure_schema['schema_v12'])
}

MODELS = MODEL_SCHEMA.keys()

NEW_LAYOUT_DATE = '2020-10-01'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/spark-output-3/'
opp1186_warehouse = 'opp_1186_warehouse'
ref_location = 's3://salusv/reference/practice_fusion/emr/'
tmp_location = '/tmp/reference/'

source_tables_repartition_key = {
    'allergen': 'allergen_id',
    'allergy': 'allergy_id',
    'appointment': 'transcript_id',
    'diagnosis': 'diagnosis_id',
    'diagnosis_icd10': 'diagnosis_id',
    'diagnosis_icd9': 'diagnosis_id',
    'diagnosis_snomed': 'diagnosis_id',
    'enccat': '',
    'encounter': 'transcript_id',
    'enctype': '',
    'lab_result': 'laborder_id',
    'laborder': 'laborder_id',
    'matching_payload': 'claimid',
    'medication': 'medication_id',
    'patient': 'patient_id',
    'patient_smoke': 'smoke_id',
    'pharmacy': 'pharmacy_id',
    'practice': '',
    'prescription': 'prescription_id',
    'provider': '',
    'smoke': 'smoke_id',
    'specialty': '',
    'transcript': 'transcript_id',
    'transcript_allergy': 'allergy_id',
    'transcript_diagnosis': 'diagnosis_id',
    'transcript_prescription': 'prescription_id',
    'vaccination': 'vaccination_id',
    'vaccine': 'vaccine_id',
    'vendor': 'vendor_id',
    '': '',
    None: None
}


def apply_trimmy_nullify(table, spark, partitions):
    """
    applu trimmy and nully ; repartition
    """
    repart_key = source_tables_repartition_key[table.lower()]
    table_as = 'laborder' if table.lower() == 'lab_result' else table
    if table == 'matching_payload':
        matching_payload_df = spark.table(table)
        cleaned_matching_payload_df = (
            pp.compose(pp.trimmify, pp.nullify)(matching_payload_df))
        cleaned_matching_payload_df.cache_and_track(table). \
            repartition(partitions, source_tables_repartition_key[table]).createOrReplaceTempView(table_as)
    else:
        tn_df = pp.nullify(pp.trimmify(spark.table(table)),
                           ['NULL', 'Null', 'null', 'unknown', 'Unknown', 'UNKNOWN', '19000101', '']). \
            cache_and_track(table)
        if repart_key:
            tn_df.repartition(partitions, repart_key).createOrReplaceTempView(table_as)
        else:
            tn_df.repartition(partitions).createOrReplaceTempView(table_as)


def filter_out(spark, ref_loc, key_clmn, nbr_of_last_char, trans_df, date_input):
    """
    Filer out Trans
    """
    # load reference locations
    def_char = '0'.zfill(nbr_of_last_char)
    logger.log('Loading reference trans table')
    trans_df.createOrReplaceTempView('trans')
    spark.read.parquet(ref_loc).createOrReplaceTempView('ref')

    # filter out any transactions that already exist in the reference location
    logger.log('Filtering input tables')
    filter_query = """SELECT trans.* FROM trans 
        LEFT JOIN ref
            ON ref.date_input = '{dt}' and trans.{clmn} = ref.{clmn} 
                AND RIGHT(NVL(trans.{clmn}, '{def_char}'), {nbr_char}) = ref.last_char_{clmn}
        WHERE  ref.{clmn} is NULL"""
    return spark.sql(filter_query.format(
        dt=date_input, clmn=key_clmn, def_char=def_char, nbr_char=nbr_of_last_char))


def collect_sql_scripts(directory_path=None):
    """
    Collect list of SQL Files
    """
    sql_table = {}
    if directory_path:
        if directory_path[-1] != '/':
            directory_path += '/'
    else:
        directory_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[1][0]).filename) \
                             .replace(PACKAGE_PATH, "") + '/'
    scripts = [f for f in os.listdir(directory_path) if f.endswith('.sql')]

    # Compare the number of unique step numbers to the number of scripts
    if len(set([int(f.split('_')[0]) for f in scripts])) != len(scripts):
        raise Exception("At least two SQL scripts have the same step number")

    try:
        scripts = sorted(scripts, key=lambda f: int(f.split('_')[0]))
    except:
        raise Exception("At least one SQL script did not follow naming convention <step_number>_<table_name>.sql")

    for s in scripts:
        sql_table['_'.join(s.replace('.sql', '').split('_')[1:])] = s
    return sql_table


def custom_transform(driver, sql_list, key):
    """
     Transform the loaded data
    """
    logger.log('Running the normalization SQL scripts')
    running_table = sql_list[key][0]
    variables = [['VDR_FILE_DT', str(driver.date_input), False],
                 ['AVAILABLE_START_DATE', driver.available_start_date, False],
                 ['EARLIEST_SERVICE_DATE', driver.earliest_service_date, False],
                 ['EARLIEST_DIAGNOSIS_DATE', driver.earliest_diagnosis_date, False]]

    sql_table_scripts = collect_sql_scripts(driver.provider_directory_path)

    if running_table in sql_table_scripts:
        logger.log(' -loading:' + running_table)
        driver.runner.run_spark_script(sql_table_scripts[running_table],
                                       variables=copy.deepcopy(variables),
                                       source_file_path=driver.provider_directory_path,
                                       return_output=True).createOrReplaceTempView(running_table)

    for sql_table, sql_script in sql_table_scripts.items():
        if sql_table != running_table:
            logger.log(' -loading:' + sql_table)
            if sql_table in [sl[0] for sl in sql_list.values()]:
                sql_df = driver.spark.sql(
                    "SELECT * FROM {run_tbl} WHERE 1=2".format(run_tbl=running_table))
            else:
                sql_df = driver.runner. \
                    run_spark_script(sql_script, variables=copy.deepcopy(variables),
                                     source_file_path=driver.provider_directory_path,
                                     return_output=True)
            sql_df.createOrReplaceTempView(sql_table)


def stage_future_data(spark, stg_loc, key_clmn, nbr_of_last_char, master_tbl, date_input):
    """
    Stage Future Data
    """
    def_char = '0'.zfill(nbr_of_last_char)
    logger.log('Saving filtered input tables to hdfs {}'.format(master_tbl))
    query = """SELECT distinct mstr.{clmn},  
            RIGHT(NVL(mstr.{clmn},'{def_char}'), {nbr_char}) as last_char_{clmn}, '{dt}' as date_input 
        FROM {tbl} mstr""".format(
        dt=date_input, tbl=master_tbl, clmn=key_clmn, def_char=def_char, nbr_char=nbr_of_last_char)
    hdfs_utils.clean_up_output_hdfs(stg_loc)
    spark.sql(query).repartition(10).write.parquet(
        stg_loc, compression='gzip', mode='append',
        partitionBy=['date_input', 'last_char_{clmn}'.format(clmn=key_clmn)])


def run(date_input, model=None, test=False, end_to_end_test=False,
        skip_filter_duplicates=False, spark=None, runner=None):
    # init

    provider_name = 'practice_fusion'
    provider_partition_name = FEED_ID
    models = [model] if model else MODELS
    has_template_v1 = \
        datetime.strptime(date_input, '%Y-%m-%d').date() >= datetime.strptime(NEW_LAYOUT_DATE, '%Y-%m-%d').date()
    source_table_schemas = transactional_schemas_v1 if has_template_v1 else transactional_schemas

    for mdl in models:
        if test:
            logger.log('Processing for {}'.format(mdl))
            output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
            driver = MarketplaceDriver(
                provider_name,
                provider_partition_name,
                source_table_schemas,
                output_table_names_to_schemas,
                date_input,
                end_to_end_test=end_to_end_test,
                test=test,
                unload_partition_count=40,
                use_ref_gen_values=True,
                vdr_feed_id=136,
                output_to_transform_path=LOAD_INTO_TRANSFORM
            )
            driver.spark = spark
            driver.runner = runner

            script_path = __file__
            driver.input_path = file_utils.get_abs_path(
                script_path, '../../../test/providers/practice_fusion/emr/resources/input/'
            ) + '/'
            driver.matching_path = file_utils.get_abs_path(
                script_path, '../../../test/providers/practice_fusion/emr/resources/matching/'
            ) + '/'
            driver.provider_directory_path = driver.provider_directory_path + mdl + '/'
            driver.load()
            driver.transform()
        else:
            if mdl == 'clinical_observation':
                #  'allergy' - multiple chunks  (clinical_observation1)
                #  'patient_smoke', 'transcript', 'encounter' - no chunks
                #           clinical_observation2, clinical_observation3, clinical_observation4
                sql_list = {1: ('practice_fusion_emr_norm_emr_clin_obsn_1', 'allergy'),
                            2: ('practice_fusion_emr_norm_emr_clin_obsn_2', 'patient_smoke'),
                            3: ('practice_fusion_emr_norm_emr_clin_obsn_3', 'transcript'),
                            4: ('practice_fusion_emr_norm_emr_clin_obsn_4', 'encounter')
                            }

                for key in sql_list.keys():
                    logger.log('{} custom chunk processing for {}'.format(mdl, key))
                    master_tbl = sql_list[key][1]

                    # # init
                    conf_parameters = {
                        'spark.executor.memoryOverhead': 4096,
                        'spark.driver.memoryOverhead': 4096
                    }

                    this_ref_location = (ref_location + mdl + '/' + master_tbl + '/')
                    this_tmp_location = (tmp_location + mdl + '/' + master_tbl + '/')
                    has_data = any(s3_utils.list_folders(this_ref_location)) if not skip_filter_duplicates else False

                    # build odd number list chunks
                    top = 2
                    max_chunk = 10
                    chunks = [num for num in range(max_chunk) if num % top == 0] if key == 1 else [1]

                    chunk_message = ''
                    for i in chunks:
                        rng = [str(chunk).zfill(1) for chunk in range(i, i + top) if chunk < max_chunk]
                        if len(chunks) > 1:
                            chunk_message = 'Custom chunk={}'.format(str(rng))
                            logger.log('{}processing for {}'.format(chunk_message, mdl))

                        output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                        driver = MarketplaceDriver(
                            provider_name,
                            provider_partition_name,
                            source_table_schemas,
                            output_table_names_to_schemas,
                            date_input,
                            end_to_end_test=end_to_end_test,
                            test=test,
                            unload_partition_count=10,
                            use_ref_gen_values=True,
                            vdr_feed_id=136,
                            output_to_transform_path=LOAD_INTO_TRANSFORM
                        )
                        driver.provider_directory_path = driver.provider_directory_path + mdl + '/'
                        this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                            if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                        # driver.init_spark_context()
                        driver.init_spark_context(conf_parameters=conf_parameters)

                        driver.load(cache_tables=False)

                        partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

                        cmn_tables = ['transcript', 'patient', 'provider', 'practice', 'specialty']
                        if key == 1:
                            cmn_tables.extend(['allergy', 'allergen', 'medication', 'transcript_allergy'])
                        elif key == 2:
                            cmn_tables.extend(['patient_smoke', 'smoke'])
                        elif key == 4:
                            cmn_tables.extend(['encounter', 'enctype', 'enccat'])

                        logger.log('Apply custom nullify trimmify')
                        apply_trimmy_nullify('matching_payload', driver.spark, partitions)
                        for table in source_table_schemas.TABLE_CONF:
                            if table.lower() in cmn_tables:
                                apply_trimmy_nullify(table, driver.spark, partitions)

                        clmn = '{}_id'.format(master_tbl)
                        trans = driver.spark.table(master_tbl)
                        if len(chunks) > 1:
                            trans = trans.filter(FN.substring(clmn, -1, 1).isin(rng))

                        if has_data:
                            trans = filter_out(driver.spark, this_ref_location, clmn, 1, trans, date_input)
                        trans = trans.repartition(partitions, clmn).cache_and_track(master_tbl)
                        trans_cnt = trans.count() or 0
                        trans.createOrReplaceTempView(master_tbl)

                        if trans_cnt > 0:
                            if len(chunks) > 1:
                                t, c = ('transcript_allergy', 'allergy_id')
                                driver.spark.table(t).filter(FN.substring(c, -1, 1).isin(rng)). \
                                    cache_and_track(t).createOrReplaceTempView(t)

                            """
                            Transform the loaded data
                            """
                            custom_transform(driver, sql_list, key)
                            driver.save_to_disk()

                            stage_df = driver.spark.read.parquet(
                                constants.hdfs_staging_dir +
                                driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                            staged_cnt = stage_df.count() or 0
                            if staged_cnt > 0:
                                stage_future_data(driver.spark, this_tmp_location, clmn, 1, master_tbl, date_input)
                                driver.stop_spark()
                                driver.log_run()
                                driver.copy_to_output_path(output_location=this_output_location)
                                # Copy claims to reference location
                                logger.log("Writing claims to the reference location for future duplication checking")
                                normalized_records_unloader.distcp(this_ref_location, src=this_tmp_location)
                                logger.log('.....{}process has been completed for {}'.format(chunk_message, mdl))
                            else:
                                driver.stop_spark()
                                logger.log('.....{}there is no data staged for {}'.format(chunk_message, mdl))
                        else:
                            driver.stop_spark()
                            logger.log('.....{}there is no trans data for {}'.format(chunk_message, mdl))
                    logger.log('.....{} custom process has been completed for {} '.format(mdl, key))
                for clean_table in [tmp_location, constants.hdfs_staging_dir]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
                logger.log('.....{} all done ....................'.format(mdl))
            elif mdl == 'diagnosis':
                #  'diagnosis' - multiple chunks (diagnosis1)
                #  'encounter' - no chunks  (diagnosis2)

                sql_list = {1: ('practice_fusion_emr_norm_emr_pre_diag', 'diagnosis'),
                            2: ('practice_fusion_emr_norm_emr_pre_diag_2', 'encounter')
                            }

                for key in sql_list.keys():
                    logger.log('{} custom chunk processing for {}'.format(mdl, key))

                    master_tbl = sql_list[key][1]

                    this_ref_location = (ref_location + mdl + '/' + master_tbl + '/')
                    this_tmp_location = (tmp_location + mdl + '/' + master_tbl + '/')
                    has_data = any(s3_utils.list_folders(this_ref_location)) if not skip_filter_duplicates else False

                    # build odd number list chunks
                    top = 1
                    max_chunk = 10
                    chunks = [num for num in range(max_chunk) if num % top == 0] if key == 1 else [1]
                    chunk_message = ''
                    for i in chunks:
                        rng = [str(chunk).zfill(1) for chunk in range(i, i + top) if chunk < max_chunk]
                        if len(chunks) > 1:
                            chunk_message = 'Custom chunk={}'.format(str(rng))
                            logger.log('{}processing for {}'.format(chunk_message, mdl))

                        output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                        driver = MarketplaceDriver(
                            provider_name,
                            provider_partition_name,
                            source_table_schemas,
                            output_table_names_to_schemas,
                            date_input,
                            end_to_end_test=end_to_end_test,
                            test=test,
                            unload_partition_count=10,
                            use_ref_gen_values=True,
                            vdr_feed_id=136,
                            output_to_transform_path=LOAD_INTO_TRANSFORM
                        )
                        driver.provider_directory_path = driver.provider_directory_path + mdl + '/'
                        this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                            if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                        driver.init_spark_context()

                        driver.load(cache_tables=False)

                        partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

                        cmn_tables = ['transcript', 'patient', 'provider', 'practice', 'specialty']
                        if key == 1:
                            cmn_tables.extend(['diagnosis', 'transcript_diagnosis', 'diagnosis_icd9', 'diagnosis_icd10',
                                               'diagnosis_snomed'])
                        elif key == 2:
                            cmn_tables.extend(['encounter', 'enctype', 'enccat'])

                        logger.log('Apply custom nullify trimmify')
                        apply_trimmy_nullify('matching_payload', driver.spark, partitions)
                        for table in source_table_schemas.TABLE_CONF:
                            if table.lower() in cmn_tables:
                                apply_trimmy_nullify(table, driver.spark, partitions)

                        clmn = '{}_id'.format(master_tbl)
                        trans = driver.spark.table(master_tbl)
                        if len(chunks) > 1:
                            trans = trans.filter(FN.substring(clmn, -1, 1).isin(rng))

                        if has_data:
                            trans = filter_out(driver.spark, this_ref_location, clmn, 1, trans, date_input)
                        trans = trans.repartition(partitions, clmn).cache_and_track(master_tbl)
                        trans_cnt = trans.count() or 0
                        trans.createOrReplaceTempView(master_tbl)

                        if trans_cnt > 0:
                            if len(chunks) > 1:
                                for t, c in [('transcript_diagnosis', 'diagnosis_id'), ('diagnosis_icd9', 'diagnosis_id'),
                                             ('diagnosis_icd10', 'diagnosis_id'), ('diagnosis_snomed', 'diagnosis_id')]:
                                    driver.spark.table(t).filter(FN.substring(c, -1, 1).isin(rng)).\
                                        cache_and_track(t).createOrReplaceTempView(t)

                            """
                            Transform the loaded data
                            """
                            custom_transform(driver, sql_list, key)
                            driver.save_to_disk()

                            stage_df = driver.spark.read.parquet(
                                constants.hdfs_staging_dir +
                                driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                            staged_cnt = stage_df.count() or 0
                            if staged_cnt > 0:
                                stage_future_data(driver.spark, this_tmp_location, clmn, 1, master_tbl, date_input)
                                driver.stop_spark()
                                driver.log_run()
                                driver.copy_to_output_path(output_location=this_output_location)
                                # Copy claims to reference location
                                logger.log("Writing claims to the reference location for future duplication checking")
                                normalized_records_unloader.distcp(this_ref_location, src=this_tmp_location)
                                logger.log('.....{}process has been completed for {}'.format(chunk_message, mdl))
                            else:
                                driver.stop_spark()
                                logger.log('.....{}there is no data staged for {}'.format(chunk_message, mdl))
                        else:
                            driver.stop_spark()
                            logger.log('.....{}there is no trans data for {}'.format(chunk_message, mdl))
                    logger.log('.....{} custom process has been completed for {} '.format(mdl, key))

                for clean_table in [tmp_location, constants.hdfs_staging_dir]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
                logger.log('.....{} all done ....................'.format(mdl))
            elif mdl == 'encounter':
                # 'transcript' - no chunks (encounter1)
                #  'appointment' - multiple chunks (encounter2)

                sql_list = {1: ('practice_fusion_emr_norm_emr_enc_1', 'transcript'),
                            2: ('practice_fusion_emr_norm_emr_enc_2', 'appointment')
                            }

                for key in sql_list.keys():
                    logger.log('{} custom chunk processing for {}'.format(mdl, key))

                    master_tbl = sql_list[key][1]

                    # # init
                    # conf_parameters = {
                    #     'spark.default.parallelism': 1000,
                    #     'spark.sql.shuffle.partitions': 1000,
                    #     'spark.executor.memoryOverhead': 1024,
                    #     'spark.driver.memoryOverhead': 1024,
                    #     'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
                    #     'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
                    #     'spark.sql.autoBroadcastJoinThreshold': 104857600,
                    #     'spark.shuffle.sasl.timeout': 60000,
                    #     'spark.task.maxFailures': 8,
                    #     'spark.max.executor.failures': 800
                    # }

                    conf_parameters = {
                        'spark.executor.memoryOverhead': 1024,
                        'spark.driver.memoryOverhead': 1024
                    }

                    this_ref_location = (ref_location + mdl + '/' + master_tbl + '/')
                    this_tmp_location = (tmp_location + mdl + '/' + master_tbl + '/')
                    has_data = any(s3_utils.list_folders(this_ref_location)) if not skip_filter_duplicates else False

                    # build odd number list chunks
                    top = 1
                    max_chunk = 10
                    chunks = [num for num in range(max_chunk) if num % top == 0] if key == 2 else [1]
                    chunk_message = ''
                    for i in chunks:
                        rng = [str(chunk).zfill(1) for chunk in range(i, i + top) if chunk < max_chunk]
                        if len(chunks) > 1:
                            chunk_message = 'Custom chunk={}'.format(str(rng))
                            logger.log('{}processing for {}'.format(chunk_message, mdl))

                        output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                        driver = MarketplaceDriver(
                            provider_name,
                            provider_partition_name,
                            source_table_schemas,
                            output_table_names_to_schemas,
                            date_input,
                            end_to_end_test=end_to_end_test,
                            test=test,
                            unload_partition_count=10,
                            use_ref_gen_values=True,
                            vdr_feed_id=136,
                            output_to_transform_path=LOAD_INTO_TRANSFORM
                        )

                        this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                            if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None
                        driver.provider_directory_path = driver.provider_directory_path + mdl + '/'

                        driver.init_spark_context(conf_parameters=conf_parameters)

                        driver.load(cache_tables=False)

                        partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

                        cmn_tables = ['transcript', 'patient', 'provider', 'practice', 'specialty',
                                      'encounter', 'enctype', 'enccat', 'appointment']
                        logger.log('Loading external table: gen_ref_whtlst')
                        external_table_loader.load_analytics_db_table(
                            driver.runner.sqlContext, 'dw', 'gen_ref_whtlst', 'gen_ref_whtlst')

                        logger.log('Apply custom nullify trimmify')
                        apply_trimmy_nullify('matching_payload', driver.spark, partitions)
                        for table in source_table_schemas.TABLE_CONF:
                            if table.lower() in cmn_tables:
                                apply_trimmy_nullify(table, driver.spark, partitions)

                        clmn = '{}_id'.format(master_tbl)
                        trans = driver.spark.table(master_tbl)
                        if len(chunks) > 1:
                            trans = trans.filter(FN.substring(clmn, -1, 1).isin(rng))

                        if has_data:
                            trans = filter_out(driver.spark, this_ref_location, clmn, 1, trans, date_input)
                        trans = trans.repartition(partitions, clmn).cache_and_track(master_tbl)
                        trans_cnt = trans.count() or 0
                        trans.createOrReplaceTempView(master_tbl)

                        if trans_cnt > 0:
                            """
                            Transform the loaded data
                            """
                            custom_transform(driver, sql_list, key)
                            driver.save_to_disk()

                            stage_df = driver.spark.read.parquet(
                                constants.hdfs_staging_dir +
                                driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                            staged_cnt = stage_df.count() or 0
                            if staged_cnt > 0:
                                stage_future_data(driver.spark, this_tmp_location, clmn, 1, master_tbl, date_input)
                                driver.stop_spark()
                                driver.log_run()
                                driver.copy_to_output_path(output_location=this_output_location)
                                # Copy claims to reference location
                                logger.log("Writing claims to the reference location for future duplication checking")
                                normalized_records_unloader.distcp(this_ref_location, src=this_tmp_location)
                                logger.log('.....{}process has been completed for {}'.format(chunk_message, mdl))
                            else:
                                driver.stop_spark()
                                logger.log('.....{}there is no data staged for {}'.format(chunk_message, mdl))
                        else:
                            driver.stop_spark()
                            logger.log('.....{}there is no trans data for {}'.format(chunk_message, mdl))
                    logger.log('.....{} custom process has been completed for {} '.format(mdl, key))
                for clean_table in [tmp_location, constants.hdfs_staging_dir]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
                logger.log('.....{} all done ....................'.format(mdl))
            elif mdl in ['lab_test', 'procedure']:
                #  no chunks
                #  'lab_test.laborder' --lab_test
                #  'procedure.vaccination', 'procedure.encounter' --procedure

                logger.log('Custom chunk processing for {}'.format(mdl))

                master_tbl = {}
                if mdl == 'lab_test':
                    master_tbl = {1: 'laborder'}
                elif mdl == 'procedure':
                    master_tbl = {1: 'vaccination', 2: 'encounter'}

                # init
                conf_parameters = {
                    'spark.executor.memoryOverhead': 2048,
                    'spark.driver.memoryOverhead': 2048
                }

                this_ref_location = {}
                this_tmp_location = {}
                has_data = {}
                for p in master_tbl:
                    this_ref_location[p] = (ref_location + mdl + '/' + master_tbl[p] + '/')
                    this_tmp_location[p] = (tmp_location + mdl + '/' + master_tbl[p] + '/')
                    has_data[p] = \
                        any(s3_utils.list_folders(this_ref_location[p])) if not skip_filter_duplicates else False

                # build odd number list chunks
                chunks = [1]
                chunk_message = ''
                for i in chunks:
                    rng = [str(i).zfill(1)]
                    if len(chunks) > 1:
                        chunk_message = 'Custom chunk={}'.format(str(rng))
                        logger.log('{}processing for {}'.format(chunk_message, mdl))

                    output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                    prtn_cnt = 10 if mdl == 'procedure' else 20

                    driver = MarketplaceDriver(
                        provider_name,
                        provider_partition_name,
                        source_table_schemas,
                        output_table_names_to_schemas,
                        date_input,
                        end_to_end_test=end_to_end_test,
                        test=test,
                        unload_partition_count=prtn_cnt,
                        use_ref_gen_values=True,
                        vdr_feed_id=136,
                        output_to_transform_path=LOAD_INTO_TRANSFORM
                    )
                    this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                        if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                    driver.provider_directory_path = driver.provider_directory_path + mdl + '/'

                    # driver.init_spark_context()
                    driver.init_spark_context(conf_parameters=conf_parameters)

                    driver.load(cache_tables=False)

                    partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

                    cmn_tables = ['patient', 'provider', 'practice', 'specialty']
                    if mdl == 'lab_test':
                        cmn_tables.extend(['lab_result', 'laborder', 'vendor'])
                    elif mdl == 'procedure':
                        cmn_tables.extend(['vaccination', 'vaccine', 'transcript', 'encounter', 'enctype', 'enccat'])

                    logger.log('Apply custom nullify trimmify')
                    apply_trimmy_nullify('matching_payload', driver.spark, partitions)
                    for table in source_table_schemas.TABLE_CONF:
                        if table.lower() in cmn_tables:
                            apply_trimmy_nullify(table, driver.spark, partitions)

                    trans_cnt = {}
                    for p in master_tbl:
                        clmn = '{}_id'.format(master_tbl[p])
                        trans = driver.spark.table(master_tbl[p])
                        if len(chunks) > 1:
                            trans = trans.filter(FN.substring(clmn, -1, 1).isin(rng))
                        if has_data[p]:
                            trans = filter_out(driver.spark, this_ref_location[p], clmn, 1, trans, date_input)

                        trans = trans.repartition(int(
                            driver.spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), clmn)
                        trans = trans.cache_and_track(master_tbl[p])
                        trans_cnt[p] = trans.count() or 0
                        trans.createOrReplaceTempView(master_tbl[p])

                    if any(cnt > 0 for cnt in trans_cnt.values()):
                        driver.transform()
                        driver.save_to_disk()

                        stage_df = driver.spark.read.parquet(
                            constants.hdfs_staging_dir +
                            driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                        staged_cnt = stage_df.count() or 0

                        if staged_cnt > 0:
                            for p in master_tbl:
                                stage_future_data(
                                    driver.spark, this_tmp_location[p],
                                    '{}_id'.format(master_tbl[p]), 1, master_tbl[p], date_input)
                            driver.stop_spark()
                            driver.log_run()
                            driver.copy_to_output_path(output_location=this_output_location)
                            # Copy claims to reference location
                            logger.log("Writing claims to the reference location for future duplication checking")
                            for p in master_tbl:
                                normalized_records_unloader.distcp(this_ref_location[p], src=this_tmp_location[p])
                                logger.log('.....{}process has been completed for {}'.format(chunk_message, mdl))
                        else:
                            driver.stop_spark()
                            logger.log('.....{}there is no data staged for {}'.format(chunk_message, mdl))
                    else:
                        driver.stop_spark()
                        logger.log('.....{}there is no trans data for {}'.format(chunk_message, mdl))

                for clean_table in [tmp_location, constants.hdfs_staging_dir]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
                logger.log('.....Custom process has been completed for {} '.format(mdl))
            elif mdl == 'medication':
                #  'medication.prescription' - multiple chunks  (medication)
                logger.log('Custom chunk processing for {}'.format(mdl))

                master_tbl = 'prescription'
                # init
                conf_parameters = {
                    'spark.executor.memoryOverhead': 4096,
                    'spark.driver.memoryOverhead': 4096
                }

                this_ref_location = ref_location + mdl + '/' + master_tbl + '/'
                this_tmp_location = tmp_location + mdl + '/' + master_tbl + '/'
                has_data = any(s3_utils.list_folders(this_ref_location)) if not skip_filter_duplicates else False

                parquet_tmp_location = tmp_location + mdl + '/parquet/'

                # build list of chunks
                list_of_tables = ['prescription', 'pharmacy', 'patient', 'provider', 'practice', 'specialty',
                                  'medication', 'transcript_prescription', 'transcript', 'diagnosis_icd9',
                                  'diagnosis_icd10']

                list_of_tables_joins = [
                    ('pharmacy', 'pharmacy_id', 'prescription', 'pharmacy_id', 10),
                    ('medication', 'medication_id', 'prescription', 'medication_id', 1),
                    ('diagnosis_icd9', 'diagnosis_id', 'prescription', 'diagnosis_id', 10),
                    ('diagnosis_icd10', 'diagnosis_id', 'prescription', 'diagnosis_id', 10),
                    ('patient', 'patient_id', 'prescription', 'patient_id', 80),
                    ('transcript_prescription', 'prescription_id', 'prescription', 'prescription_id', 80),
                    ('transcript', 'transcript_id', 'transcript_prescription', 'transcript_id', 200),
                    ('provider', 'provider_id', 'transcript', 'provider_id', 1),
                    ('practice', 'practice_id', 'provider', 'practice_id', 1),
                    ('specialty', 'specialty_id', 'provider', 'primary_specialty_id', 1),
                    ('matching_payload', 'claimid', 'patient', 'patient_id', 80)
                ]
                """
                START
                """
                output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                driver = MarketplaceDriver(
                    provider_name,
                    provider_partition_name,
                    source_table_schemas,
                    output_table_names_to_schemas,
                    date_input,
                    end_to_end_test=end_to_end_test,
                    test=test,
                    unload_partition_count=5,
                    use_ref_gen_values=True,
                    vdr_feed_id=136,
                    output_to_transform_path=LOAD_INTO_TRANSFORM
                )
                this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                    if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                driver.provider_directory_path = driver.provider_directory_path + mdl + '/'

                # driver.init_spark_context()
                driver.init_spark_context(conf_parameters=conf_parameters)

                partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

                driver.load(cache_tables=False)
                logger.log('Apply custom nullify trimmify')
                apply_trimmy_nullify('matching_payload', driver.spark, partitions)
                for table in source_table_schemas.TABLE_CONF:
                    if table.lower() in list_of_tables:
                        apply_trimmy_nullify(table, driver.spark, partitions)

                logger.log('Parquet staging processing started for {}'.format(mdl))
                p_sql = """SELECT prx.*, RIGHT(NVL(prx.prescription_id,'00'), 2) as last_char_prescription_id 
                    FROM prescription prx 
                    WHERE TRIM(UPPER(COALESCE(prx.prescription_id, 'empty'))) <> 'PRESCRIPTION_ID' """
                loc = '{}{}/'.format(parquet_tmp_location, master_tbl)
                hdfs_utils.clean_up_output_hdfs(loc)
                driver.spark.sql(p_sql).repartition(50).write.parquet(
                    loc, compression='gzip', mode='overwrite', partitionBy=['last_char_prescription_id'])

                for dim_tbl, dim_clmn, lkp_tbl, lkp_clmn, rpart_num in list_of_tables_joins:
                    loc = '{}{}/'.format(parquet_tmp_location, dim_tbl)
                    p_sql = """SELECT dim.* 
                    FROM {dim_tbl} dim 
                    WHERE exists (
                        SELECT * FROM {lkp_tbl} lkp 
                            WHERE COALESCE(lkp.{lkp_clmn}, 'NULL') = COALESCE(dim.{dim_clmn}, 'empty')
                            )
                            """.format(dim_tbl=dim_tbl, dim_clmn=dim_clmn, lkp_tbl=lkp_tbl, lkp_clmn=lkp_clmn)
                    hdfs_utils.clean_up_output_hdfs(loc)
                    driver.spark.sql(p_sql).repartition(rpart_num).write.parquet(
                        loc, compression='gzip', mode='overwrite')
                driver.stop_spark()
                logger.log('Parquet staging processing has been completed for {}'.format(mdl))
                """
                END 
                """

                top = 4
                max_chunk = 100
                chunks = [num for num in range(max_chunk) if num % top == 0]
                for i in chunks:
                    rng = [str(chunk).zfill(2) for chunk in range(i, i + top) if chunk < max_chunk]

                    logger.log('Custom chunk={} processing for {}'.format(str(rng), mdl))

                    output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                    driver = MarketplaceDriver(
                        provider_name,
                        provider_partition_name,
                        source_table_schemas,
                        output_table_names_to_schemas,
                        date_input,
                        end_to_end_test=end_to_end_test,
                        test=test,
                        unload_partition_count=5,
                        use_ref_gen_values=True,
                        vdr_feed_id=136,
                        output_to_transform_path=LOAD_INTO_TRANSFORM
                    )
                    this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                        if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                    driver.provider_directory_path = driver.provider_directory_path + mdl + '/'

                    # driver.init_spark_context()
                    driver.init_spark_context(conf_parameters=conf_parameters)

                    partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))
                    driver.spark.read.parquet('{}{}/'.format(parquet_tmp_location, master_tbl)). \
                        repartition(partitions, 'prescription_id'). \
                        cache_and_track(master_tbl).createOrReplaceTempView(master_tbl)

                    trans = driver.spark.table(master_tbl)
                    trans = trans.filter(FN.substring('prescription_id', -2, 2).isin(rng))
                    if has_data:
                        trans = filter_out(driver.spark, this_ref_location, 'prescription_id', 2, trans, date_input)

                    trans = trans.repartition(int(
                        driver.spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')))
                    # trans = trans.repartition(int(
                    #     driver.spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'prescription_id')
                    trans = trans.cache_and_track(master_tbl)
                    trans_cnt = trans.count() or 0
                    trans.createOrReplaceTempView(master_tbl)

                    if trans_cnt > 0:
                        for dim_tbl, dim_clmn, lkp_tbl, lkp_clmn, rpart_num in list_of_tables_joins:
                            loc = '{}{}/'.format(parquet_tmp_location, dim_tbl)
                            driver.spark.read.parquet(loc).createOrReplaceTempView(dim_tbl)
                            p_sql = """SELECT dim.* FROM {dim_tbl} dim WHERE exists (select * from {lkp_tbl} lkp 
                                WHERE COALESCE(lkp.{lkp_clmn}, 'NULL') = COALESCE(dim.{dim_clmn}, 'empty')) 
                                """.format(dim_tbl=dim_tbl, dim_clmn=dim_clmn, lkp_tbl=lkp_tbl, lkp_clmn=lkp_clmn)
                            driver.spark.sql(p_sql).repartition(rpart_num). \
                                cache_and_track(dim_tbl).createOrReplaceTempView(dim_tbl)

                        driver.transform()
                        driver.save_to_disk()

                        stage_df = driver.spark.read.parquet(
                            constants.hdfs_staging_dir +
                            driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                        staged_cnt = stage_df.count() or 0

                        if staged_cnt > 0:
                            stage_future_data(
                                driver.spark, this_tmp_location, 'prescription_id', 2, 'prescription', date_input)
                            driver.stop_spark()
                            driver.log_run()
                            driver.copy_to_output_path(output_location=this_output_location)
                            # Copy claims to reference location
                            logger.log("Writing claims to the reference location for future duplication checking")
                            normalized_records_unloader.distcp(this_ref_location, src=this_tmp_location)
                            logger.log('.....Custom chunk={} process has been completed for {}'.format(str(rng), mdl))
                        else:
                            driver.stop_spark()
                            logger.log('.....Custom chunk={} there is no data staged for {}'.format(str(rng), mdl))
                    else:
                        driver.stop_spark()
                        logger.log('.....Custom chunk={} there is no trans data for {}'.format(str(rng), mdl))

                for clean_table in [tmp_location, constants.hdfs_staging_dir, parquet_tmp_location]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
                logger.log('.....Custom process has been completed for {} '.format(mdl))
            else:
                logger.log('Custom processing for {}'.format(mdl))
                # driver.unload_partition_count = 40

                output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}
                driver = MarketplaceDriver(
                    provider_name,
                    provider_partition_name,
                    source_table_schemas,
                    output_table_names_to_schemas,
                    date_input,
                    end_to_end_test=end_to_end_test,
                    test=test,
                    unload_partition_count=20,
                    use_ref_gen_values=True,
                    vdr_feed_id=136,
                    output_to_transform_path=LOAD_INTO_TRANSFORM
                )
                this_output_location = driver.output_path.replace('/parquet/', opp1186_warehouse) \
                    if LOAD_OUTPUT_OPP_DW and not driver.output_to_transform_path else None

                driver.provider_directory_path = driver.provider_directory_path + mdl + '/'

                driver.init_spark_context()
                driver.load()

                matching_payload_df = driver.spark.table('matching_payload')
                cleaned_matching_payload_df = (
                    pp.compose(pp.trimmify, pp.nullify)(matching_payload_df))
                cleaned_matching_payload_df.cache_and_track('matching_payload').createOrReplaceTempView(
                    'matching_payload')

                logger.log('Apply custom nullify trimmify')
                for table in source_table_schemas.TABLE_CONF:
                    pp.nullify(
                        pp.trimmify(driver.spark.table(table))
                        , ['NULL', 'Null', 'null', 'unknown', 'Unknown', 'UNKNOWN', '19000101', '']) \
                        .cache_and_track(table).createOrReplaceTempView(table)

                driver.transform()
                driver.save_to_disk()
                driver.stop_spark()
                driver.log_run()
                driver.copy_to_output_path(output_location=this_output_location)


def main(args):
    models = args.models.split(',') if args.models else MODELS

    for model in models:
        run(args.date, model, end_to_end_test=args.end_to_end_test, skip_filter_duplicates=args.skip_filter_duplicates)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--models', help='Comma-separated list of models to normalize instead of all models')
    parser.add_argument('--skip_filter_duplicates', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
