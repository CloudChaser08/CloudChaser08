"""
vigilanz labtests normalize
"""
import os
import argparse
import pyspark.sql.functions as FN
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.lab_test import schemas as lab_test_schema
from spark.common.emr.medication import schemas as medication_schemas
import spark.providers.vigilanz.emr.transactional_schemas_v1 as transactional_schemas_v1
from spark.common.marketplace_driver import MarketplaceDriver
import spark.helpers.constants as constants
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.common.utility import logger


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


def stage_future_data(spark, stg_loc, key_clmn, nbr_of_last_char, trans_master_tbl, date_input):
    """
    Stage Future Data
    """
    def_char = '0'.zfill(nbr_of_last_char)
    logger.log('Saving filtered input tables to hdfs {}'.format(trans_master_tbl))
    query = """SELECT distinct mstr.{clmn},  
            RIGHT(NVL(mstr.{clmn},'{def_char}'), {nbr_char}) as last_char_{clmn}, '{dt}' as date_input 
        FROM {tbl} mstr""".format(
        dt=date_input, tbl=trans_master_tbl, clmn=key_clmn, def_char=def_char, nbr_char=nbr_of_last_char)
    hdfs_utils.clean_up_output_hdfs(stg_loc)
    spark.sql(query).repartition(10).write.parquet(
        stg_loc, compression='gzip', mode='append',
        partitionBy=['date_input', 'last_char_{clmn}'.format(clmn=key_clmn)])


HAS_DELIVERY_PATH = False
ref_location = 's3://salusv/reference/practice_fusion/emr/'
tmp_location = '/tmp/reference/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'vigilanz'

    MODEL_SCHEMA = {
        'diagnosis': ('vigilanz_emr_norm_diag_final', diagnosis_schemas['schema_v10']),
        'clinical_observation': ('vigilanz_emr_norm_clin_obsn_final', clinical_observation_schemas['schema_v11']),
        'encounter': ('vigilanz_emr_norm_enc_final', encounter_schemas['schema_v10']),
        'medication': ('vigilanz_emr_norm_medctn_final', medication_schemas['schema_v11']),
        'lab_test': ('vigilanz_emr_norm_lab_test_final', lab_test_schema['schema_v3'])
    }

    # key: model_schema name  values (key table name and key table's column name)
    MODEL_CHUNK = {
        'medication': ('medication_administration', 'row_id')
    }

    provider_partition_name = '250'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--models', help='Comma-separated list of models to normalize instead of all models')
    parser.add_argument('--skip_filter_duplicates', default=False, action='store_true')
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    skip_filter_duplicates = args.skip_filter_duplicates
    end_to_end_test = args.end_to_end_test

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 2048,
        'spark.driver.memoryOverhead': 2048,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 104857600
    }

    # v1 Transactional Schemas
    source_table_schemas = transactional_schemas_v1

    # collect keys
    models_chunk = MODEL_CHUNK.keys()
    MODELS = MODEL_SCHEMA.keys()
    models = args.models.split(',') if args.models else MODELS

    for mdl in models:
        output_table_names_to_schemas = {MODEL_SCHEMA[mdl][0]: MODEL_SCHEMA[mdl][1]}

        this_ref_location = (ref_location + mdl + '/' + MODEL_SCHEMA[mdl][0] + '/')
        this_tmp_location = (tmp_location + mdl + '/' + MODEL_SCHEMA[mdl][0] + '/')
        has_data = any(s3_utils.list_folders(this_ref_location)) if not skip_filter_duplicates else False

        # build odd number list chunks
        top = 1
        max_chunk = 10
        chunks = [num for num in range(max_chunk) if num % top == 0] if mdl in models_chunk else [1]
        chunk_message = ''
        for i in chunks:
            rng = [str(chunk).zfill(1) for chunk in range(i, i + top) if chunk < max_chunk]
            if len(chunks) > 1:
                chunk_message = 'Custom chunk={}'.format(str(rng))
                logger.log('{}processing for {}'.format(chunk_message, mdl))

            # Create and run driver
            driver = MarketplaceDriver(
                provider_name,
                provider_partition_name,
                source_table_schemas,
                output_table_names_to_schemas,
                date_input,
                end_to_end_test,
                unload_partition_count=5,
                vdr_feed_id=250,
                use_ref_gen_values=True,
                output_to_delivery_path=HAS_DELIVERY_PATH,
                output_to_transform_path=False
            )

            driver.init_spark_context(conf_parameters=conf_parameters)
            driver.load()
            additional_variables=[['CHUNK', str(i), False]]
            if mdl not in models_chunk:
                driver.transform(additional_variables=additional_variables)
                driver.save_to_disk()
                driver.stop_spark()
                driver.log_run()
                driver.copy_to_output_path()
            else:
                partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))
                trans_master_tbl = MODEL_CHUNK[mdl][0]
                clmn = MODEL_CHUNK[mdl][1]
                trans = driver.spark.table(trans_master_tbl)

                if len(chunks) > 1:
                    trans = trans.filter(FN.substring(clmn, 1-len(max_chunk), len(max_chunk)-1).isin(rng))

                if has_data:
                    trans = filter_out(driver.spark, this_ref_location, clmn, len(max_chunk)-1, trans, date_input)
                trans = trans.repartition(partitions, clmn).cache_and_track(trans_master_tbl)
                trans_cnt = trans.count() or 0
                trans.createOrReplaceTempView(trans_master_tbl)

                if trans_cnt > 0:
                    """
                    Transform the loaded data
                    """
                    driver.transform(additional_variables=additional_variables)
                    driver.save_to_disk()

                    stage_df = driver.spark.read.parquet(
                        constants.hdfs_staging_dir +
                        driver.output_table_names_to_schemas[MODEL_SCHEMA[mdl][0]].output_directory)
                    staged_cnt = stage_df.count() or 0
                    if staged_cnt > 0:
                        stage_future_data(driver.spark, this_tmp_location, clmn, len(max_chunk)-1, trans_master_tbl, date_input)
                        driver.stop_spark()
                        driver.log_run()
                        driver.copy_to_output_path()
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
                logger.log('.....{} custom process has been completed'.format(mdl))
                for clean_table in [tmp_location, constants.hdfs_staging_dir]:
                    hdfs_utils.clean_up_output_hdfs(clean_table)
            logger.log('.....{} all done ....................'.format(mdl))
