"""MarketplaceRunner is a generic runner for HVM normalization routines"""
import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader

GENERIC_MINIMUM_DATE = datetime.date(1901, 1, 1)
END_TO_END_TEST = 'end_to_end_test'
TEST = 'test'
PRODUCTION = 'production'
DRIVER_MODULE_NAME = 'driver'
SCRIPT_PATH = __file__
E2E_PATH = 's3://salusv/testing/dewey/airflow/e2e/{provider_name}/{data_type}/{year}/{month:02d}/{day:02d}/'

MODE_RECORDS_PATH_TEMPLATE = {
    TEST: './test/marketplace/resources/records/',
    END_TO_END_TEST: E2E_PATH + 'records/',
    PRODUCTION: 's3://salusv/incoming/{data_type}/{provider_name}/{year}/{month:02d}/{day:02d}/'
}

MODE_MATCHING_PATH_TEMPLATE = {
    TEST: './test/marketplace/resources/matching/',
    END_TO_END_TEST: E2E_PATH + 'matching/',
    PRODUCTION: 's3://salusv/matching/payload/{data_type}/{provider_name}/{year}/{month:02d}/{day:02d}/'
}

MODE_OUTPUT_PATH = {
    TEST: './test/marketplace/resources/output/',
    END_TO_END_TEST: E2E_PATH + 'output/',
    PRODUCTION: 's3://salusv/warehouse/parquet/{data_type}/'
}


class MarketplaceDriver(object):
    """
    Marketplace Diver to load, transform and save provider data.
    """
    def __init__(self,
                 provider_name,
                 data_type,
                 date_input,
                 script_path,
                 provider_directory_path,
                 source_table_schema,
                 output_table_names_to_schemas,
                 provider_partition_column,
                 date_partition_column,
                 test=False,
                 end_to_end_test=False,
                 distribution_key='record_id'):
        self.provider_name = provider_name
        self.data_type = data_type
        self.date_input = datetime.datetime.strptime(date_input, '%Y-%m-%d').date()
        self.script_path = script_path
        self.provider_directory_path = provider_directory_path
        self.test = test
        self.end_to_end_test = end_to_end_test
        self.source_table_schema = source_table_schema
        self.output_table_names_to_schemas = output_table_names_to_schemas
        self.provider_partition_column = provider_partition_column
        self.date_partition_column = date_partition_column
        self.distribution_key = distribution_key
        self.input_path = None
        self.matching_path = None
        self.output_path = None
        self.spark = None
        self.sql_context = None
        self.runner = None

        if self.test:
            mode = TEST
        elif self.end_to_end_test:
            mode = END_TO_END_TEST
        else:
            mode = PRODUCTION

        # get i/o paths
        self.input_path = MODE_RECORDS_PATH_TEMPLATE[mode].format(
            provider_name=self.provider_name, data_type=self.data_type,
            year=self.date_input.year, month=self.date_input.month, day=self.date_input.day
        )
        self.matching_path = MODE_MATCHING_PATH_TEMPLATE[mode].format(
            provider_name=self.provider_name, data_type=self.data_type,
            year=self.date_input.year, month=self.date_input.month, day=self.date_input.day
        )
        self.output_path = MODE_OUTPUT_PATH[mode].format(
            provider_name=self.provider_name, data_type=self.data_type,
            year=self.date_input.year, month=self.date_input.month, day=self.date_input.day
        )

    def init_spark_context(self):
        if not self.spark:
            context_list = [str(self.date_input), self.provider_name, self.data_type, 'HVM']
            context_name = ' '.join(context_list)
            print('Starting {context_name}'.format(context_name=context_name))
            self.spark, self.sql_context = init(context_name, self.test)
            self.runner = Runner(self.sql_context)

    def run(self):
        """
        Kick off the load, transform, create workflow
        """
        self.init_spark_context()
        print('Starting Step 1 of 3: Loading data')
        self.load()
        print('Starting Step 2 of 3: Transforming data')
        self.transform()
        print('Starting Step 3 of 3: Save data')
        self.save()
        self.spark.stop()

    def load(self):
        """
        Load the input data into tables
        """
        records_loader.load_and_clean_all_v2(self.runner, self.input_path, self.source_table_schema,
                                             load_file_name=True)
        payload_loader.load(self.runner, self.matching_path, load_file_name=True)
        if not self.test:
            external_table_loader.load_ref_gen_ref(self.runner.sqlContext)
            external_table_loader.load_analytics_db_table(
                self.runner.sqlContext, 'dw', 'date_explode_indices', 'date_explode_indices'
            )
            self.spark.table('date_explode_indices').cache() \
                .createOrReplaceTempView('date_explode_indices')
            self.spark.table('date_explode_indices').count()

    def transform(self):
        """
        Transform the loaded data
        """
        self.runner.run_all_spark_scripts([['VDR_FILE_DT', str(self.date_input), False]],
                                          directory_path=self.provider_directory_path)

    def save(self):
        """
        Ensure the transformed data conforms to a known data schema and unload the data
        """
        for table in self.output_table_names_to_schemas.keys():
            data_frame = self.spark.table(table)
            schema_obj = self.output_table_names_to_schemas[table]
            output = schema_enforcer.apply_schema(data_frame,
                                                  schema_obj.schema_structure,
                                                  columns_to_keep=[self.provider_partition_column,
                                                                   self.date_partition_column])

            _columns = data_frame.columns
            _columns.remove(self.provider_partition_column)
            _columns.remove(self.date_partition_column)
            normalized_records_unloader.unload(
                self.spark, self.runner, output, self.date_partition_column, str(self.date_input),
                self.provider_name, substr_date_part=False, columns=_columns,
                distribution_key=self.distribution_key
            )
            normalized_records_unloader.distcp(self.output_path + schema_obj.output_folder)
