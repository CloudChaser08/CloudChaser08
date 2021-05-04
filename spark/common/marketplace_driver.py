"""MarketplaceRunner is a generic runner for HVM normalization routines"""
import os
import inspect
import datetime
import argparse

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.common.utility.logger as logger
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility.output_type import DataType, RunType
import spark.helpers.postprocessor as pp
import spark.helpers.constants as constants
import spark.helpers.s3_constants as s3_constants
import subprocess

GENERIC_MINIMUM_DATE = datetime.date(1901, 1, 1)
END_TO_END_TEST = 'end_to_end_test'
TEST = 'test'
PRODUCTION = 'production'
TRANSFORM = 'transform'
RESTRICTED = 'restricted'
DRIVER_MODULE_NAME = 'driver'

MODE_RECORDS_PATH_TEMPLATE = {
    TEST: './test/marketplace/resources/records/',
    END_TO_END_TEST: s3_constants.RECORDS_PATH,
    PRODUCTION: s3_constants.RECORDS_PATH,
    TRANSFORM: s3_constants.RECORDS_PATH,
    RESTRICTED: s3_constants.RECORDS_PATH
}

MODE_MATCHING_PATH_TEMPLATE = {
    TEST: './test/marketplace/resources/matching/',
    END_TO_END_TEST: s3_constants.MATCHING_PATH,
    PRODUCTION: s3_constants.MATCHING_PATH,
    TRANSFORM: s3_constants.MATCHING_PATH,
    RESTRICTED: s3_constants.MATCHING_PATH
}

MODE_OUTPUT_PATH = {
    TEST: './test/marketplace/resources/output/',
    END_TO_END_TEST: s3_constants.E2E_OUTPUT_PATH,
    PRODUCTION: s3_constants.PRODUCTION_PATH,
    TRANSFORM: s3_constants.TRANSFORM_PATH,
    RESTRICTED: s3_constants.RESTRICTED_PATH
}


class MarketplaceDriver(object):
    """
    Marketplace Driver to load, transform and save provider data.
    """
    def __init__(self,
                 provider_name,
                 provider_partition_name,
                 source_table_schema,
                 output_table_names_to_schemas,
                 date_input,
                 end_to_end_test=False,
                 test=False,
                 load_date_explode=True,
                 output_to_transform_path=False,
                 unload_partition_count=20,
                 vdr_feed_id=None,
                 use_ref_gen_values=False,
                 count_transform_sql=False,
                 restricted_private_source=False,
                 additional_output_path=None, # additional_output_schemas are written to this exact s3 key
                 additional_output_schemas=None # dict with same keys as output_table_names_to_schemas
                 ):

        # get directory and path for provider
        previous_stack_frame = inspect.currentframe().f_back

        provider_directory_path = os.path.dirname(
            inspect.getframeinfo(previous_stack_frame).filename)

        provider_directory_path = provider_directory_path.replace('spark/target/dewey.zip/', "") + '/'

        self.first_schema_name = list(output_table_names_to_schemas.keys())[0]
        self.first_schema_obj = output_table_names_to_schemas[self.first_schema_name]

        self.provider_name = provider_name
        self.provider_partition_name = provider_partition_name
        self.data_type = self.first_schema_obj.data_type
        self._data_type_str = DataType(self.data_type).value
        self.date_input = datetime.datetime.strptime(date_input, '%Y-%m-%d').date()
        self.provider_directory_path = provider_directory_path
        self.test = test
        self.end_to_end_test = end_to_end_test
        self.source_table_schema = source_table_schema
        self.output_table_names_to_schemas = output_table_names_to_schemas
        self.load_date_explode = load_date_explode
        self.output_to_transform_path = output_to_transform_path
        self.unload_partition_count = unload_partition_count
        self.vdr_feed_id = vdr_feed_id
        self.use_ref_gen_values = use_ref_gen_values
        self.count_transform_sql = count_transform_sql
        self.restricted_private_source = restricted_private_source
        self.additional_output_path = additional_output_path
        self.additional_output_schemas = additional_output_schemas
        self.available_start_date = None
        self.earliest_service_date = None
        self.input_path = None
        self.matching_path = None
        self.output_path = None
        self.spark = None
        self.sql_context = None
        self.runner = None

        # set running mode
        if self.test:
            mode = TEST
        elif self.end_to_end_test:
            mode = END_TO_END_TEST
        elif self.output_to_transform_path:
            mode = TRANSFORM
        elif self.restricted_private_source:
            mode = RESTRICTED
        else:
            mode = PRODUCTION

        # get i/o paths
        self.input_path = MODE_RECORDS_PATH_TEMPLATE[mode].format(
            provider_name=self.provider_name, data_type=self._data_type_str,
            year=self.date_input.year, month=self.date_input.month, day=self.date_input.day
        )
        self.matching_path = MODE_MATCHING_PATH_TEMPLATE[mode].format(
            provider_name=self.provider_name, data_type=self._data_type_str,
            year=self.date_input.year, month=self.date_input.month, day=self.date_input.day
        )

        self.output_path = MODE_OUTPUT_PATH[mode]
        self.parse_args()

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--output_to_transformed", default=False, action='store_true')
        args = parser.parse_known_args()[0]
        
        self.output_to_transform_path = args.output_to_transformed or self.output_to_transform_path

    def init_spark_context(self, conf_parameters=None):
        if not self.spark:
            context_list = [str(self.date_input), self.provider_name, self._data_type_str, 'HVM']
            context_name = ' '.join(context_list)
            logger.log('Starting {context_name}'.format(context_name=context_name))
            self.spark, self.sql_context = init(context_name, self.test, conf_parameters)
            self.runner = Runner(self.sql_context)

    def run(self, conf_parameters=None):
        """
        Run all driver steps in the appropriate order
        """
        self.init_spark_context(conf_parameters)
        self.load()
        self.transform()
        self.save_to_disk()
        self.log_run()
        self.stop_spark()
        self.copy_to_output_path()

    def load(self, extra_payload_cols=None, cache_tables=True, payloads=True):
        """
        Load the input data into tables
        """
        logger.log('Loading the source data')
        logger.log(' -loading: transactions')
        records_loader.load_and_clean_all_v2(self.runner, self.input_path, self.source_table_schema,
                                             load_file_name=True, cache_tables=cache_tables,
                                             spark_context=self.spark)
        if payloads:
            logger.log(' -loading: payloads')
            payload_loader.load(self.runner, self.matching_path, load_file_name=True,
                                extra_cols=extra_payload_cols)
        if not self.test:
            logger.log(' -loading: ref_gen_ref')
            external_table_loader.load_ref_gen_ref(self.runner.sqlContext)
            if self.use_ref_gen_values:
                self.get_ref_gen_ref_values()

            if self.load_date_explode:
                logger.log(' -loading: date_explode_indices')
                external_table_loader.load_analytics_db_table(
                    self.runner.sqlContext, 'dw', 'date_explode_indices', 'date_explode_indices'
                )
                self.spark.table('date_explode_indices').cache() \
                    .createOrReplaceTempView('date_explode_indices')

    def get_ref_gen_ref_values(self):
        if not self.vdr_feed_id:
            self.stop_spark()
            raise AttributeError("load_ref_gen_values requires a valid vdr_feed_id")

        self.earliest_service_date = pp.get_gen_ref_date(self.spark,
                                                         self.vdr_feed_id,
                                                         'EARLIEST_VALID_SERVICE_DATE',
                                                         get_as_string=True)
        self.available_start_date = pp.get_gen_ref_date(self.spark,
                                                        self.vdr_feed_id,
                                                        'HVM_AVAILABLE_HISTORY_START_DATE',
                                                        get_as_string=True)

    def transform(self):
        """
        Transform the loaded data
        """
        logger.log('Running the normalization SQL scripts')

        variables = [['VDR_FILE_DT', str(self.date_input), False],
                     ['AVAILABLE_START_DATE', self.available_start_date, False],
                     ['EARLIEST_SERVICE_DATE', self.earliest_service_date, False]]
        self.runner.run_all_spark_scripts(variables, directory_path=self.provider_directory_path,
                                          count_transform_sql=self.count_transform_sql)

    def apply_schema(self, data_frame, schema_obj):
        output = schema_enforcer.apply_schema(
            data_frame,
            schema_obj.schema_structure,
            columns_to_keep=[
                schema_obj.provider_partition_column,
                schema_obj.date_partition_column
            ]
        )

        return output

    def unload(self, data_frame, schema_obj, columns, table):
        normalized_records_unloader.unload(
            self.spark,
            self.runner,
            data_frame,
            schema_obj.date_partition_column,
            str(self.date_input),
            self.provider_partition_name,
            substr_date_part=False,
            columns=columns,
            date_partition_name=schema_obj.date_partition_column,
            provider_partition_name=schema_obj.provider_partition_column,
            distribution_key=schema_obj.distribution_key,
            staging_subdir=schema_obj.output_directory,
            partition_by_part_file_date=self.output_to_transform_path,
            unload_partition_count=self.unload_partition_count,
            test_dir=(self.output_path if self.test else None)
        )

    def save_schema_to_disk(self, data_frame, schema_obj, table):
        """
        Saves another version of the table to disk defined & partitioned with a specified schema.
        """
        logger.log('Saving data to local file system with schema {}'.format(schema_obj.name))

        output = self.apply_schema(data_frame, schema_obj)

        _columns = output.columns
        _columns.remove(schema_obj.provider_partition_column)
        _columns.remove(schema_obj.date_partition_column)

        self.unload(data_frame=output, schema_obj=schema_obj, columns=_columns, table=table)
        output.unpersist()

    def save_to_disk(self):
        """
        Ensure the transformed data conforms to a known data schema and unload the data locally
        """
        logger.log('Saving data to the local file system')
        for table in self.output_table_names_to_schemas.keys():
            data_frame = self.spark.table(table)
            schema_obj = self.output_table_names_to_schemas[table]

            self.save_schema_to_disk(data_frame, schema_obj, table)

            if self.additional_output_path and self.additional_output_schemas:
                self.save_schema_to_disk(data_frame, self.additional_output_schemas[table], table)

            data_frame.unpersist()
            

    def log_run(self):
        logger.log('Logging run details')
        if not self.test and not self.end_to_end_test:
            logger.log_run_details(
                self.provider_name,
                self.data_type,
                self.input_path,
                self.matching_path,
                self.output_path,
                RunType.MARKETPLACE,
                self.date_input
            )

    def stop_spark(self):
        """
        Stop the spark context
        """
        logger.log('Stopping the spark context')
        self.spark.stop()

    def copy_to_multiple_output_paths(self):
        for table in self.output_table_names_to_schemas.keys():
            schema_obj = self.output_table_names_to_schemas[table]
            additional_schema_obj = self.additional_output_schemas[table]

            default_src = constants.hdfs_staging_dir + schema_obj.output_directory
            default_dest = self.output_path + schema_obj.output_directory

            additional_src = constants.hdfs_staging_dir + additional_schema_obj.output_directory
            additional_dest = self.additional_output_path + additional_schema_obj.output_directory

            if not self.test and not self.end_to_end_test:
                hadoop_time = normalized_records_unloader.timed_distcp(dest=default_dest, src=default_src)
                RunRecorder().record_run_details(additional_time=hadoop_time)

                hadoop_time = normalized_records_unloader.timed_distcp(dest=additional_dest, src=additional_src)
                RunRecorder().record_run_details(additional_time=hadoop_time)          
            
            elif self.end_to_end_test:
                normalized_records_unloader.distcp(dest=default_dest, src=default_src)
                normalized_records_unloader.distcp(dest=additional_dest, src=additional_src)    

    def copy_to_output_path(self, output_location=None):
        """
        Copy data from local file system to output destination
        """
        if self.output_path and self.additional_output_path and self.additional_output_schemas:
            self.copy_to_multiple_output_paths()
            return

        if not output_location:
            output_location = self.output_path

        logger.log('Copying data to the output location: {}'.format(output_location))

        if not self.test and not self.end_to_end_test:
            hadoop_time = normalized_records_unloader.timed_distcp(output_location)
            RunRecorder().record_run_details(additional_time=hadoop_time)
        
        elif self.end_to_end_test:
            normalized_records_unloader.distcp(output_location)

    def move_output_to_backup(self, output_location, backup_location=None):
        """
        Moves existing data on S3 to a backup location, usually before driver.copy_to_output_path()
        NOTE: This function clears out the backup location before moving data there, so make sure backup_path isn't an important location!
        """
        if not backup_location:
            backup_location = output_location.replace('salusv', 'salusv/backup')
        
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', backup_location])
        subprocess.check_call(['aws', 's3', 'mv', '--recursive', output_location, backup_location])
        
