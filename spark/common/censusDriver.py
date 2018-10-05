import datetime
import inspect
import importlib

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.common.std_census as std_census

GENERIC_MINIMUM_DATE = datetime.date(1901, 1, 1)
TEST                 = 'test'
END_TO_END_TEST      = 'end_to_end_test'
PRODUCTION           = 'production'
DRIVER_MODULE_NAME   = 'driver'
PACKAGE_PATH         = 'spark/target/dewey.zip/'

class SetterProperty(object):
    def __init__(self, func, doc=None):
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__
    def __set__(self, obj, value):
        return self.func(obj, value)

class CensusDriver(object):
    """
    Base class for census routine drivers
    """
    def __init__(self, client_name, opportunity_id, test=False, end_to_end_test=False):
        self._client_name       = client_name
        self._opportunity_id    = opportunity_id
        self._test              = test
        self._end_to_end_test   = end_to_end_test

        self._records_path_template     = None
        self._matching_path_template    = None
        self._output_path_template      = None
        self._output_file_name_template = 'response_{year}{month:02d}{day:02d}.gz'

        self._records_module_name           = 'records_schemas'
        self._matching_payloads_module_name = 'matching_paylods_schemas'

        self._script_path = None

        if test:
            mode = TEST
        elif end_to_end_test:
            mode = END_TO_END_TEST
        else:
            mode = PRODUCTION

        mode_records_path_template = {
            TEST            : '../../../test/census/{client}/{opp_id}/resources/input/{{year}}/{{month:02d}}/{{day:02d}}/',
            END_TO_END_TEST : 's3://salusv/testing/dewey/airflow/e2e/{client}/{opp_id}/records/{{year}}/{{month:02d}}/{{day:02d}}/',
            PRODUCTION      : 's3a://salusv/incoming/census/{client}/{opp_id}/{{year}}/{{month:02d}}/{{day:02d}}/'
        }

        mode_matching_path_template = {
            TEST            : '../../../test/census/{client}/{opp_id}/resources/matching/{{year}}/{{month:02d}}/{{day:02d}}/',
            END_TO_END_TEST : 's3://salusv/testing/dewey/airflow/e2e/{client}/{opp_id}/matching/{{year}}/{{month:02d}}/{{day:02d}}/',
            PRODUCTION      : 's3a://salusv/matching/payload/census/{client}/{opp_id}/{{year}}/{{month:02d}}/{{day:02d}}/'
        }

        mode_output_path_template = {
            TEST            : '../../../test/census/{client}/{opp_id}/resources/output/{{year}}/{{month:02d}}/{{day:02d}}/',
            END_TO_END_TEST : 's3://salusv/testing/dewey/airflow/e2e/{client}/{opp_id}/output/{{year}}/{{month:02d}}/{{day:02d}}/',
            PRODUCTION      : 's3a://salusv/deliverable/{client}/{opp_id}/{{year}}/{{month:02d}}/{{day:02d}}/'
        }

        # init
        self._spark, self._sqlContext = init("{} {} Census".format(self._client_name, self._opportunity_id))
        self._runner = Runner(self._sqlContext)

        self._records_path_template  = mode_records_path_template[mode].format(
                client=self._client_name, opp_id=self._opportunity_id
            )
        self._matching_path_template = mode_matching_path_template[mode].format(
                client=self._client_name, opp_id=self._opportunity_id
            )
        self._output_path_template = mode_output_path_template[mode].format(
                client=self._client_name, opp_id=self._opportunity_id
            )

    # Overwrite default records path template
    @SetterProperty
    def records_path_template(self, path_template):
        self.__dict__['_records_path_template'] = path_template

    # Overwrite default matching paylaods path template
    @SetterProperty
    def matching_path_template(self, path_template):
        self.__dict__['_matching_path_template'] = path_template

    # Overwrite default output path template
    @SetterProperty
    def output_path_template(self, path_template):
        self.__dict__['_output_path_template'] = path_template

    # Overwrite default records module name
    @SetterProperty
    def records_module_name(self, module_name):
        self.__dict__['_records_module_name'] = module_name

    # Overwrite default matching payloads module name
    @SetterProperty
    def matching_payloads_module_name(self, module_name):
        self.__dict__['_matching_payloads_module_name'] = module_name

    def load(self, batch_date):
        records_schemas           = importlib.import_module(
                self.__module__.replace(DRIVER_MODULE_NAME, self._records_module_name)
            )
        matching_payloads_schemas = importlib.import_module(
                self.__module__.replace(DRIVER_MODULE_NAME, self._matching_payloads_module_name)
            )

        records_path  = self._records_path_template.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )
        matching_path = self._matching_path_template.format(
            year=batch_date.year, month=batch_date.month, day=batch_date.day
        )

        if self._test:
            # Tests run on local files
            records_path  = file_utils.get_abs_path(__file__, records_path) + '/'
            matching_path = file_utils.get_abs_path(__file__, matching_path) + '/'

        records_loader.load_and_clean_all_v2(self._runner, records_path,
                records_schemas, load_file_name=True)
        payload_loader.load_all(self._runner, matching_path,
                matching_payloads_schemas)

    def transform(self):
        # Since this module are in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        scripts_directory = '/'.join(inspect.getfile(std_census).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])
        content = self._runner.run_all_spark_scripts(variables=[['opp_id', self._opportunity_id]],
                directory_path=scripts_directory)
        header = self._sqlContext.createDataFrame([content.columns], schema=content.schema)
        return header.union(content).coalesce(1)

    def save(self, dataframe, batch_date):
        dataframe.createOrReplaceTempView('deliverable')
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, 'hdfs:///staging/{year}/{month:02d}/{day:02d}/'.format(
                year=batch_date.year, month=batch_date.month, day=batch_date.day
            ),
            'deliverable',
            output_file_name=self._output_file_name.format(
                batch_date.year, batch_date.month, batch_date.day
            )
        )

    def copy_to_s3(self, batch_date):
        output_path = self._output_path_template.format(
            batch_date.year, batch_date.month, batch_date.day
        )
        if self._test:
            output_path = file_utils.get_abs_path(__file__, output_path) + '/'

        normalized_records_unloader.distcp(output_path)

