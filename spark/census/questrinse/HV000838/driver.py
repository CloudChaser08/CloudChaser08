from math import ceil
import re

from spark.common.census_driver import CensusDriver
import spark.common.utility.logger as logger
import spark.helpers.hdfs_tools as hdfs_utils
import spark.helpers.file_utils as file_utils
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.census.questrinse.HV000838.refbuild_loinc_delta_sql as refbuild_loinc_delta

PARQUET_FILE_SIZE = 1024 * 1024 * 1024
REFERENCE_OUTPUT_PATH = 's3://salusv/reference/questrinse/loinc_ref/'
REFERENCE_HDFS_OUTPUT_PATH = '/reference/'
REFERENCE_LOINC_DELTA = "loinc_delta"


class QuestRinseCensusDriver(CensusDriver):
    def load(self, batch_date, batch_id, chunk_records_files=None):
        super().load(batch_date, batch_id, chunk_records_files)

        logger.log('Loading external table: ref_geo_state')
        external_table_loader.load_analytics_db_table(
            self._sqlContext, 'dw', 'ref_geo_state', 'ref_geo_state'
        )
        self._spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')
        self._spark.table('ref_geo_state').count()

        logger.log('Loading LOINC reference data from S3')
        self._spark.read.parquet(REFERENCE_OUTPUT_PATH).cache().createOrReplaceTempView('loinc')
        self._spark.table('loinc').count()

        df = self._spark.table('order_result')
        df = df.repartition(int(self._spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'unique_accession_id')
        df = df.cache_and_track('order_result')
        df.createOrReplaceTempView('order_result')
        df.count()

        logger.log('Building LOINC delta reference data')
        file_utils.clean_up_output_hdfs(REFERENCE_HDFS_OUTPUT_PATH)
        for table_conf in refbuild_loinc_delta.TABLE_CONF:
            table_name = str(table_conf['table_name'])
            logger.log("        -loading: writing {}".format(table_name))
            repart_num = 20 if table_name == 'lqrc_order_result_trans' else 1
            sql_stmnt = str(table_conf['sql_stmnt']).strip()
            if len(sql_stmnt) == 0:
                continue
            self._runner.run_spark_query(sql_stmnt, return_output=True).createOrReplaceTempView(table_name)
            if table_name == REFERENCE_LOINC_DELTA:
                self._spark.table(table_name).repartition(repart_num).write.parquet(
                    REFERENCE_HDFS_OUTPUT_PATH + table_name, compression='gzip', mode='append', partitionBy=['year'])
            else:
                self._spark.table(table_name).repartition(repart_num).write.parquet(
                    REFERENCE_HDFS_OUTPUT_PATH + table_name, compression='gzip', mode='append')
                self._spark.read.parquet(
                    REFERENCE_HDFS_OUTPUT_PATH + table_name).cache().createOrReplaceTempView(table_name)

        if hdfs_utils.list_parquet_files(REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)[0].strip():
            logger.log('Loading Delta LOINC reference data from HDFS')
            self._spark.read.parquet(REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA).cache().createOrReplaceTempView(REFERENCE_LOINC_DELTA)
        else:
            logger.log('No Delta LOINC reference data for this cycle')
            self._spark.table("loinc").cache().createOrReplaceTempView(REFERENCE_LOINC_DELTA)
        self._spark.table(REFERENCE_LOINC_DELTA).count()

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        # This data goes right to the provider. They want the data in parquet without
        # column partitions.
        logger.log('Saving data to the local file system')
        dataframe.persist()
        dataframe.count()
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        local_output_path = '/staging/{batch_id_path}/'.format(batch_id_path=_batch_id_path)
        dataframe.repartition(100).write.parquet(local_output_path, compression='gzip', mode='overwrite')

        # Delivery requirement: max file size of 1GB
        # Calculate number of partitions required to maintain a max file size of 1GB
        repartition_cnt = int(ceil(hdfs_utils.get_hdfs_file_path_size(local_output_path) / PARQUET_FILE_SIZE))
        logger.log('Repartition into {} partitions'.format(repartition_cnt))

        dataframe.repartition(repartition_cnt).write.parquet(local_output_path, compression='gzip', mode='overwrite')

        logger.log("Renaming files")
        output_file_name_template = 'Data_Set_{}_response_{{:05d}}.gz.parquet'.format(batch_id)

        for filename in [f for f in hdfs_utils.get_files_from_hdfs_path('hdfs://' + local_output_path)
                         if not f.startswith('.') and f != "_SUCCESS"]:
            part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
            new_name = output_file_name_template.format(int(part_number))
            file_utils.rename_file_hdfs(local_output_path + filename, local_output_path + new_name)

        logger.log('Creating manifest file with counts')
        manifest_file_name = 'Data_Set_{}_manifest.tsv'.format(batch_id)
        file_utils.create_parquet_row_count_file(
            self._spark, local_output_path,
            self._output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(batch_id_path=_batch_id_path),
            manifest_file_name, True
        )

    def copy_to_s3(self, batch_date=None, batch_id=None):
        super().copy_to_s3(batch_date, batch_id)

        if hdfs_utils.list_parquet_files(REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)[0].strip():
            logger.log("Copying reference files to: " + REFERENCE_OUTPUT_PATH)
            normalized_records_unloader.distcp(REFERENCE_OUTPUT_PATH, REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)
        logger.log('Deleting ' + REFERENCE_HDFS_OUTPUT_PATH)
        file_utils.clean_up_output_hdfs(REFERENCE_HDFS_OUTPUT_PATH)

        # Quest doesn't want to see the _SUCCESS file that spark prints out
        logger.log('Deleting _SUCCESS file')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        file_utils.delete_success_file(self._output_path.replace('s3a:', 's3:') + '{batch_id_path}/'.format(batch_id_path=_batch_id_path))
