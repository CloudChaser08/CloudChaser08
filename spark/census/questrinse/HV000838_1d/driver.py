"""driver schema questrince hv000838_1b"""
from math import ceil
import re
import importlib
import inspect

from spark.common.census_driver import CensusDriver
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.file_utils as file_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import pyspark.sql.functions as FN
import spark.census.questrinse.HV000838_1d.refbuild_loinc_delta_sql as refbuild_loinc_delta
import spark.census.questrinse.HV000838_1d.udf as GET_RESULT_VALUE
from spark.runner import PACKAGE_PATH

PARQUET_FILE_SIZE = 1024 * 1024 * 1024
REFERENCE_OUTPUT_PATH = 's3://salusv/reference/questrinse/{}/'
REF_LOINC = "loinc_ref"
REF_RESULT_VALUE = "result_value_ref"

# QDIP config.
QDIP_LOAD = False
QDIP_PARQUET_FILE_SIZE = 1024 * 1024 * 350
QDIP_COLUMNS = [
    'record_id', 'HV_claim_id', 'hvid', 'obfuscate_hvid', 'HV_patient_gender',
    'HV_patient_age', 'HV_patient_year_of_birth', 'HV_patient_state', 'date_service',
    'HV_date_report', 'hv_loinc_code', 'result_id',
    'result', 'result_name', 'result_unit_of_measure',
    'result_desc', 'HV_ref_range_alpha', 'HV_s_diag_code_codeset_ind',
    'HV_ordering_npi', 'ordering_specialty', 'HV_ordering_state',
    'diag_date_of_service', 'reportable_results_ind', 'abnormal_ind',
    'alpha_normal_flag', 'ref_range_low', 'ref_range_high',
    'ref_range_alpha', 'date_final_report', 'HV_result_value_operator',
    'HV_result_value_numeric', 'HV_result_value_alpha', 'HV_result_value',
    'lab_code', 'daard_client_flag', 'hv_abnormal_indicator', 'hv_act_spclty_desc_cmdm'
]

REF_HDFS_OUTPT_PATH = '/reference/'
REF_LOINC_DELTA = "loinc_delta"
REF_RSLT_VAL_DELTA = "result_value_lkp"

QDIP_LOCAL_STAGING = '/qdip_staging/'
POC_1B1 = True
poc_output_path = 's3://salusv/deliverable/questrinse_1b/1d_sample_20220304/'

DIAG_CD_CLMNS = ['unique_accession_id', 's_diag_code', 's_icd_codeset_ind']
DIAG_CLMNS = ['unique_accession_id', 'accn_id', 'date_of_service', 'lab_code', 'acct_id', 'acct_number', 'dos_yyyymm']


class QuestRinseCensusDriver(CensusDriver):
    def load(self, batch_date, batch_id, chunk_records_files=None):
        hdfs_utils.clean_up_output_hdfs('/staging/')
        # hdfs_utils.clean_up_output_hdfs(REF_HDFS_OUTPT_PATH)
        super().load(batch_date, batch_id, chunk_records_files)

        matching_payload_df = self._spark.table('matching_payload')

        cleaned_matching_payload_df = (
            postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))

        cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

        logger.log('Loading external table: ref_geo_state')
        external_table_loader.load_analytics_db_table(
            self._sqlContext, 'dw', 'ref_geo_state', 'ref_geo_state'
        )
        self._spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')
        self._spark.table('ref_geo_state').count()

        logger.log('Loading LOINC reference data from S3')
        self._spark.read.parquet(REFERENCE_OUTPUT_PATH.format(REF_LOINC)).cache() \
            .createOrReplaceTempView('quest_loinc')

        loinc_dedup_sql = """
        SELECT
            date_of_service,
            upper_result_name,
            local_result_code,
            units,
            MAX(loinc_code) AS loinc_code,
            year
        FROM quest_loinc

        GROUP BY 
            date_of_service,
            upper_result_name,
            local_result_code,
            units,
            year
        ORDER BY 
            date_of_service,
            upper_result_name,
            local_result_code,
            units,
            year
        """
        self._spark.sql(loinc_dedup_sql).createOrReplaceTempView('loinc')
        self._spark.table('loinc').count()

        # logger.log('Loading external table: ref_questrinse_qtim1')
        # self._spark.sql("refresh table default.ref_questrinse_qtim1")
        # external_table_loader.load_analytics_db_table(
        #     self._sqlContext, 'default', 'ref_questrinse_qtim1', 'ref_questrinse_qtim1'
        # )
        # self._spark.table('ref_questrinse_qtim1').cache().createOrReplaceTempView('qtim1')
        #
        # logger.log('Loading external table: ref_questrinse_qtim2')
        # self._spark.sql("refresh table default.ref_questrinse_qtim2")
        # external_table_loader.load_analytics_db_table(
        #     self._sqlContext, 'default', 'ref_questrinse_qtim2', 'ref_questrinse_qtim2'
        # )
        # self._spark.table('ref_questrinse_qtim2').cache().createOrReplaceTempView('qtim2')

        logger.log('Loading external table: ref_questrinse_qtim')
        external_table_loader.load_analytics_db_table(
            self._sqlContext, 'default', 'ref_questrinse_qtim', 'ref_questrinse_qtim'
        )
        cleaned_ref_questrinse_qtim_df = postprocessor. \
            nullify(postprocessor.trimmify(self._spark.table('ref_questrinse_qtim')),
                    ['NULL', 'Null', 'null', 'N/A', ''])
        cleaned_ref_questrinse_qtim_df.createOrReplaceTempView("ref_questrinse_qtim")

        # self._spark.read.parquet('/tmp/qr/questrinse_cmdm/').createOrReplaceTempView('ref_questrinse_cmdm')
        self._spark.read.parquet('s3://salusv/reference/parquet/ref_cmdm_quest/file_date=2022-02-20/') \
            .distinct().cache().createOrReplaceTempView('ref_questrinse_cmdm')

        self._spark.read.parquet('s3://salusv/reference/parquet/ref_physicians_quest/file_date=2022-02-20/') \
            .distinct().cache().createOrReplaceTempView('ref_questrinse_physicians')

        self._sqlContext.setConf("spark.driver.memoryOverhead", 16384)
        self._sqlContext.setConf("spark.executor.memoryOverhead", 16384)
        self._sqlContext.setConf("spark.default.parallelism", 1000)
        self._sqlContext.setConf("spark.sql.shuffle.partitions", 1000)
        self._sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", 26240000)

        df = self._spark.table('order_result')
        df = df.repartition(int(
            self._spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')),
            'unique_accession_id')
        df = df.cache_and_track('order_result')
        df.createOrReplaceTempView('order_result')
        df.count()

        diag_df = self._spark.table('diagnosis')
        tbl = 'diagnosis_skinny'
        logger.log('Saving {}'.format(tbl))
        diag_cd_cleaned_df = diag_df.select(*DIAG_CD_CLMNS).distinct()
        diag_cd_cleaned_df.repartition(2).write.parquet(
            '/tmp/qr/{}/'.format(tbl), compression='gzip', mode='overwrite')
        self._spark.read.parquet('/tmp/qr/{}/'.format(tbl)).createOrReplaceTempView(tbl)

        tbl = 'diagnosis'
        logger.log('Saving {}'.format(tbl))
        diag_cleaned_df = diag_df.select(*DIAG_CLMNS).distinct()
        diag_cleaned_df.repartition(2).write.parquet(
            '/tmp/qr/{}/'.format(tbl), compression='gzip', mode='overwrite')
        self._spark.read.parquet('/tmp/qr/{}/'.format(tbl)).createOrReplaceTempView(tbl)

        logger.log('Building LOINC delta reference data')
        for table_conf in refbuild_loinc_delta.TABLE_CONF:
            table_name = str(table_conf['table_name'])
            logger.log("        -loading: writing {}".format(table_name))
            repart_num = 20 if table_name == 'lqrc_order_result_trans' else 1
            sql_stmnt = str(table_conf['sql_stmnt']).strip()
            if not sql_stmnt:
                continue
            self._runner.run_spark_query(sql_stmnt, return_output=True).createOrReplaceTempView(table_name)
            if table_name == REF_LOINC_DELTA:
                self._spark.table(table_name).repartition(repart_num).write.parquet(
                    REF_HDFS_OUTPT_PATH + table_name, compression='gzip',
                    mode='append', partitionBy=['year'])
            else:
                self._spark.table(table_name).repartition(repart_num).write.parquet(
                    REF_HDFS_OUTPT_PATH + table_name,
                    compression='gzip', mode='append')
                self._spark.read.parquet(
                    REF_HDFS_OUTPT_PATH + table_name).cache() \
                    .createOrReplaceTempView(table_name)

        if hdfs_utils.list_parquet_files(REF_HDFS_OUTPT_PATH + REF_LOINC_DELTA)[0].strip():
            logger.log('Loading Delta LOINC reference data from HDFS')
            self._spark.read.parquet(REF_HDFS_OUTPT_PATH + REF_LOINC_DELTA) \
                .cache().createOrReplaceTempView(REF_LOINC_DELTA)
            self._spark.read.parquet(REF_HDFS_OUTPT_PATH + REF_LOINC_DELTA) \
                .createOrReplaceTempView('loinc_delta_new')
            loinc_delta_dedup_sql = """
            SELECT
                date_of_service,
                upper_result_name,
                local_result_code,
                units,
                MAX(loinc_code) AS loinc_code,
                year
            FROM loinc_delta_new
            GROUP BY
                date_of_service,
                upper_result_name,
                local_result_code,
                units,
                year
            ORDER BY
                date_of_service,
                upper_result_name,
                local_result_code,
                units,
                year
            """
            self._spark.sql(loinc_delta_dedup_sql).createOrReplaceTempView(REF_LOINC_DELTA)
        else:
            logger.log('No Delta LOINC reference data for this cycle')
            self._spark.table("loinc").cache().createOrReplaceTempView(REF_LOINC_DELTA)
        logger.log('No Delta LOINC reference data for this cycle')
        self._spark.table("loinc").cache().createOrReplaceTempView(REF_LOINC_DELTA)

        self._spark.table(REF_LOINC_DELTA).count()

        logger.log('Building UDF-result value intermediate table')
        census_module = importlib.import_module(self._base_package)
        scripts_directory = '/'.join(inspect.getfile(census_module).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])

        GOLD_CLMNS = ['gen_ref_cd', 'gen_ref_desc']
        gold_df = \
            self._runner.run_spark_script('0_labtest_quest_rinse_result_gold_alpha.sql',
                                          source_file_path=scripts_directory,
                                          return_output=True
                                          ).select(*GOLD_CLMNS)\
                                            .withColumn("new_cd",FN.upper(FN.col('gen_ref_cd')))\
                                            .withColumn("new_desc",FN.upper(FN.col('gen_ref_desc')))\
                                            .select('new_cd','new_desc').distinct()

        delta_df = self._spark.table('order_result').select('result_value') \
            .withColumn('gen_ref_cd', FN.upper(FN.col('result_value'))).select('gen_ref_cd') \
            .where(FN.col('gen_ref_cd').isNotNull() & (FN.trim(FN.col('gen_ref_cd')) != '')) \
            .distinct()

        result_value_df = delta_df.join(gold_df, gold_df['new_cd'] == delta_df['gen_ref_cd'], 'left_anti').distinct()
        gold_df.cache().createOrReplaceTempView('ref_gold_alpha')

        parse_value = GET_RESULT_VALUE.udf_gen(table='ref_gold_alpha', test=None, spark=self._spark)

        UDF_CLMNS = ['gen_ref_cd', 'udf_operator', 'udf_numeric', 'udf_alpha', 'udf_passthru']
        # Process 'result' column with UDF and Coalesce passthru's as ALPHA
        out_result_value_df = result_value_df \
            .withColumn('udf_result', parse_value(FN.col('result_value'))).cache() \
            .withColumn('udf_operator', FN.when(FN.col('udf_result')[0] != "", FN.col('udf_result')[0]).otherwise(None)) \
            .withColumn('udf_numeric', FN.when(FN.col('udf_result')[1] != "", FN.col('udf_result')[1]).otherwise(None)) \
            .withColumn('udf_alpha', FN.when(FN.col('udf_result')[2] != "", FN.col('udf_result')[2]).otherwise(None)) \
            .withColumn('udf_passthru', FN.when(FN.col('udf_result')[3] != "", FN.col('udf_result')[3]).otherwise(None)) \
            .distinct().select(*UDF_CLMNS)

        expand_gold_df = gold_df.withColumnRenamed('new_cd', 'gen_ref_cd') \
            .withColumn('udf_operator', FN.lit(None).cast('string')) \
            .withColumn('udf_numeric', FN.lit(None).cast('string')) \
            .withColumnRenamed('new_desc', 'udf_alpha') \
            .withColumn('udf_passthru', FN.lit(None).cast('string')).select(*UDF_CLMNS)

        expand_gold_df.unionAll(out_result_value_df) \
            .where(FN.col('gen_ref_cd').isNotNull() & (FN.trim(FN.col('gen_ref_cd')) != '')) \
            .distinct() \
            .repartition(1).write.parquet(REF_HDFS_OUTPT_PATH + REF_RSLT_VAL_DELTA + '/', compression='gzip',
                                          mode='overwrite')

        self._spark.read.parquet(REF_HDFS_OUTPT_PATH + REF_RSLT_VAL_DELTA + '/') \
            .createOrReplaceTempView(REF_RSLT_VAL_DELTA)

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        logger.log('Saving data to the local file system')
        dataframe.persist()
        dataframe.count()
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        local_output_path = '/staging/{batch_id_path}/'.format(batch_id_path=_batch_id_path)
        local_output_path_temp = local_output_path.replace('/staging/', '/staging_temp/')
        dataframe.repartition(100).write.parquet(local_output_path_temp,
                                                 compression='gzip', mode='overwrite')

        # Delivery requirement: max file size of 1GB
        # Calculate number of partitions required to maintain a max file size of 1GB
        repartition_cnt = int(ceil(hdfs_utils.get_hdfs_file_path_size(local_output_path_temp) / PARQUET_FILE_SIZE)) or 1
        logger.log('Repartition into {} partitions'.format(repartition_cnt))
        self._spark.read.parquet(local_output_path_temp)\
            .repartition(repartition_cnt).write.parquet(local_output_path, compression='gzip', mode='overwrite')
        hdfs_utils.clean_up_output_hdfs(local_output_path_temp)
        # dataframe.repartition(repartition_cnt).write.parquet(local_output_path,
        #                                                      compression='gzip', mode='overwrite')

        # dashboard output into warehouse location
        qdip_local_output_path = '{staging_dir}' \
                                 '{batch_id_path}/'.format(staging_dir=QDIP_LOCAL_STAGING,
                                                           batch_id_path=_batch_id_path)
        if QDIP_LOAD:
            # ------------------QDIP Load start------------------
            logger.log("Saving QDIP data to the local file system")
            # reduce file size
            qdip_repartition_cnt = int(ceil(repartition_cnt *
                                            PARQUET_FILE_SIZE /
                                            QDIP_PARQUET_FILE_SIZE / 5)) or 1

            repart_cnt = int(self._spark.sparkContext.getConf().get('spark.sql.shuffle.partitions'))

            logger.log(f'Resolved qdip_local_output_path: {qdip_local_output_path}')

            # read data from previously deliverable output
            df_rslt = self._spark.read.parquet(local_output_path) \
                .repartition(repart_cnt).withColumnRenamed('hvid', 'obfuscate_hvid')

            # read sources transactions and matching payload
            df_trans = self._spark.table('transactions')
            df_pay = self._spark.table('matching_payload')

            # collect hvid
            df_trans_pay = df_trans.join(df_pay, ['hvJoinKey']).select(
                df_trans.unique_accession_id, df_pay.hvid).distinct().repartition(repart_cnt)

            # drop existing masked hvid and add load hvid
            qdip_df = df_rslt.join(df_trans_pay, ['unique_accession_id'], 'left').select(
                [df_trans_pay.hvid] + [clmn for clmn in df_rslt.columns if
                                       clmn not in {'hvid'}]).select(QDIP_COLUMNS)
            # load into hdfs
            qdip_df.repartition(qdip_repartition_cnt).write.parquet(
                qdip_local_output_path, compression='gzip', mode='overwrite')
            # ------------------QDIP Load end------------------

        logger.log("Renaming files")
        if not POC_1B1:
            output_file_name_template = 'Data_Set_{}_response_{{:05d}}.gz.parquet'.format(batch_id)
        else:
            output_file_name_template = 'Test1d_Data_Set_{}_response_{{:05d}}.gz.parquet'.format(batch_id)

        local_output_path_list = [local_output_path]
        if QDIP_LOAD:
            local_output_path_list.append(qdip_local_output_path)

        for get_output_path in local_output_path_list:
            for filename in [f for f in
                             hdfs_utils.get_files_from_hdfs_path('hdfs://' + get_output_path)
                             if not f.startswith('.') and f != "_SUCCESS"]:
                part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
                if get_output_path == qdip_local_output_path:
                    new_name = 'Data_Set_{}_dashboard_response_{{:05d}}.gz.parquet'.format(
                        batch_id).format(int(part_number))
                else:
                    new_name = output_file_name_template.format(int(part_number))
                hdfs_utils.rename_file_hdfs(get_output_path + filename, get_output_path + new_name)

        logger.log('Creating manifest file with counts')
        if not POC_1B1:
            manifest_file_name = 'Data_Set_{}_manifest.tsv'.format(batch_id)
            manifest_file_path = self._output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(
                batch_id_path=_batch_id_path)
        else:
            manifest_file_name = 'Test1d_Data_Set_{}_manifest.tsv'.format(batch_id)
            manifest_file_path = poc_output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(
                batch_id_path=_batch_id_path)

        file_utils.create_parquet_row_count_file(
            self._spark, local_output_path,
            manifest_file_path,
            manifest_file_name, True
        )

    def copy_to_s3(self, batch_date=None, batch_id=None):
        logger.log("lets see")
        # normalized_records_unloader.distcp(poc_output_path)
        if not POC_1B1:
            # Copy census batch
            super().copy_to_s3(batch_date, batch_id)

            # Copy qdip batch
            if QDIP_LOAD:
                normalized_records_unloader.distcp(
                    src=QDIP_LOCAL_STAGING,
                    dest=self._output_path.replace('/deliverable/', '/warehouse/datamart/dashboard/'))
        else:
            # Copy census batch
            normalized_records_unloader.distcp(poc_output_path)
            # Copy qdip batch
            if QDIP_LOAD:
                normalized_records_unloader.distcp(
                    src=QDIP_LOCAL_STAGING,
                    dest=poc_output_path.replace('/deliverable/', '/warehouse/datamart/dashboard_poc/'))

        if not POC_1B1:
            if hdfs_utils.list_parquet_files(REF_HDFS_OUTPT_PATH + REF_LOINC_DELTA)[0].strip():
                logger.log("Copying reference files to: " + REFERENCE_OUTPUT_PATH.format(REF_LOINC))
                normalized_records_unloader.distcp(
                    REFERENCE_OUTPUT_PATH.format(REF_LOINC), REF_HDFS_OUTPT_PATH + REF_LOINC_DELTA)

        if hdfs_utils.list_parquet_files(REF_HDFS_OUTPT_PATH + REF_RSLT_VAL_DELTA)[0].strip():
            logger.log("Copying reference files to: " + REFERENCE_OUTPUT_PATH.format(REF_RESULT_VALUE))
            normalized_records_unloader.distcp(
                REFERENCE_OUTPUT_PATH.format(
                    REF_RESULT_VALUE) + 'batch_id={}/batch_date={}/'.format(batch_date, batch_id)
                , REF_HDFS_OUTPT_PATH + REF_RSLT_VAL_DELTA)

        logger.log('Deleting ' + REF_HDFS_OUTPT_PATH)
        hdfs_utils.clean_up_output_hdfs(REF_HDFS_OUTPT_PATH)

        # Quest doesn't want to see the _SUCCESS file that spark prints out
        logger.log('Deleting _SUCCESS file')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)

        if not POC_1B1:
            s3_utils.delete_success_file(
                self._output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(batch_id_path=_batch_id_path))
        else:
            s3_utils.delete_success_file(
                poc_output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(batch_id_path=_batch_id_path))
