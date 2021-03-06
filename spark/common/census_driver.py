"""census driver"""
import datetime
import importlib
import inspect
import os

import boto3

import spark.common.std_census as std_census
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
from spark.runner import Runner, PACKAGE_PATH
from spark.spark_setup import init
from spark.common.std_census import records_schemas, matching_payloads_schemas
from spark.common.utility.logger import log

GENERIC_MINIMUM_DATE = datetime.date(1901, 1, 1)
TEST = 'test'
END_TO_END_TEST = 'end_to_end_test'
PRODUCTION = 'production'
DRIVER_MODULE_NAME = 'driver'
SAVE_PATH = 'hdfs:///staging/'
SALUSV = 's3://salusv/'

MODE_RECORDS_PATH_TEMPLATE = {
    TEST: '../test/census/{client}/{opp_id}/resources/input/{{batch_id_path}}/',
    END_TO_END_TEST: SALUSV + 'testing/dewey/airflow/e2e/{client}/{opp_id}/records/{{batch_id_path}}/',
    PRODUCTION: 's3a://salusv/incoming/census/{client}/{opp_id}/{{batch_id_path}}/'
}

MODE_MATCHING_PATH_TEMPLATE = {
    TEST: '../test/census/{client}/{opp_id}/resources/matching/{{batch_id_path}}/',
    END_TO_END_TEST: SALUSV + 'testing/dewey/airflow/e2e/{client}/{opp_id}/matching/{{batch_id_path}}/',
    PRODUCTION: 's3a://salusv/matching/payload/census/{client}/{opp_id}/{{batch_id_path}}/'
}

MODE_OUTPUT_PATH = {
    TEST: '../test/census/{client}/{opp_id}/resources/output/',
    END_TO_END_TEST: SALUSV + 'testing/dewey/airflow/e2e/{client}/{opp_id}/output/',
    PRODUCTION: 's3a://salusv/deliverable/{client}/{opp_id}/'
}


class SetterProperty(object):
    def __init__(self, func, doc=None):
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__

    def __set__(self, obj, value):
        return self.func(obj, value)


class BotoParser:
    def __init__(self, path):
        file_path = path.replace('s3a:', 's3:')
        files_path = file_path.split('/')
        self.s3 = 's3'
        self.bucket = files_path[2]
        self.key = '/'.join(files_path[3:-1])
        self.prefix = files_path[0] + '//' + self.bucket + '/'

# Directory structure if inherting from this class
# spark/census/
#   <client_name>/
#       __init__.py
#       <opportunity_id>/
#           __init__.py
#           driver.py
#           matching_payloads_schemas.py
#           records_schemas.py
#           *.sql -- See runner.py for sql script naming conventions
#


class CensusDriver(object):
    """
    Base class for census routine drivers
    """
    def __init__(self, client_name, opportunity_id, salt=None, no_row_id=False, test=False,
                 end_to_end_test=False, base_package=None):
        self._client_name = client_name
        self._opportunity_id = opportunity_id
        self._salt = salt
        self._no_row_id = no_row_id
        self._test = test
        self._end_to_end_test = end_to_end_test
        self._base_package = base_package or self.__module__.replace('.' + DRIVER_MODULE_NAME, '')
        log("Starting Census Routine - {}: {}".format(client_name, opportunity_id))
        # if a salt is not specified, default to the opp id
        if self._salt is None:
            self._salt = opportunity_id

        self._records_path_template = None
        self._matching_path_template = None
        self._output_path = None
        self._output_file_name_template = '{batch_id_value}_response_{{part_num}}.csv.gz'

        self._records_module_name = 'records_schemas'
        self._matching_payloads_module_name = 'matching_payloads_schemas'

        self._script_path = None

        if test:
            mode = TEST
        elif end_to_end_test:
            mode = END_TO_END_TEST
        else:
            mode = PRODUCTION

        self._spark, self._sqlContext = \
            init("{} {} Census".format(self._client_name, self._opportunity_id))
        self._runner = Runner(self._sqlContext)

        self._records_path_template = MODE_RECORDS_PATH_TEMPLATE[mode].format(
            client=self._client_name, opp_id=self._opportunity_id
        )
        self._matching_path_template = MODE_MATCHING_PATH_TEMPLATE[mode].format(
            client=self._client_name, opp_id=self._opportunity_id
        )
        self._output_path = MODE_OUTPUT_PATH[mode].format(
            client=self._client_name, opp_id=self._opportunity_id
        )

    def _get_batch_info(self, batch_date, batch_id):
        if batch_id:
            _batch_id_path = batch_id
            _batch_id_value = batch_id
        else:
            _batch_id_path = '{year}/{month:02d}/{day:02d}'.format(year=batch_date.year,
                                                                   month=batch_date.month,
                                                                   day=batch_date.day)
            _batch_id_value = '{year}{month:02d}{day:02d}'.format(year=batch_date.year,
                                                                  month=batch_date.month,
                                                                  day=batch_date.day)

        return _batch_id_path, _batch_id_value

    # Overwrite default records path template
    @SetterProperty
    def records_path_template(self, path_template):
        self._records_path_template = path_template

    # Overwrite default matching paylaods path template
    @SetterProperty
    def matching_path_template(self, path_template):
        self._matching_path_template = path_template

    # Overwrite default output path template
    @SetterProperty
    def output_path(self, path):
        self._output_path = path

    # Overwrite default records module name
    @SetterProperty
    def records_module_name(self, module_name):
        self._records_module_name = module_name

    # Overwrite default matching payloads module name
    @SetterProperty
    def matching_payloads_module_name(self, module_name):
        self._matching_payloads_module_name = module_name

    def get_batch_records_files(self, batch_date, batch_id):
        """List all files within a given os or s3 directory, recursively"""

        def recurse_s3_directory_for_files(directory_path):
            """Private - List all files within a given s3 directory, recursively"""
            record_files = []
            boto_parser = BotoParser(directory_path)
            s3 = boto3.client(boto_parser.s3)
            kwargs = {'Bucket': boto_parser.bucket, 'Prefix': boto_parser.key}
            while True:
                resp = s3.list_objects_v2(**kwargs)
                try:
                    contents = resp['Contents']
                    filtered_contents = filter(lambda x: x['Size'] > 0, contents)
                    for obj in filtered_contents:
                        record_files.append(obj['Key'])
                    try:
                        kwargs['ContinuationToken'] = resp['NextContinuationToken']
                    except KeyError:
                        break
                except KeyError:
                    break  # the directory doesn't exist. Just return []
            return record_files

        def recurse_os_directory_for_files(path):
            """Private - List all files within a given os directory, recursively"""
            return [os.path.join(dp, f) for dp, dn, fn in os.walk(path) for f in fn]

        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        records_path = self._records_path_template.format(
            batch_id_path=_batch_id_path
        )
        if records_path.startswith('s3'):
            return recurse_s3_directory_for_files(records_path)
        else:
            return recurse_os_directory_for_files(records_path)

    def load(self, batch_date, batch_id, chunk_records_files=None):
        log("Loading input")

        try:
            records_schemas = importlib.import_module(
                self._base_package + '.' + self._records_module_name
            )
        except:
            records_schemas = std_census.records_schemas

        try:
            matching_payloads_schemas = importlib.import_module(
                self._base_package + '.' + self._matching_payloads_module_name
            )
        except:
            matching_payloads_schemas = std_census.matching_payloads_schemas

        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        records_path = self._records_path_template.format(
            batch_id_path=_batch_id_path
        )
        matching_path = self._matching_path_template.format(
            batch_id_path=_batch_id_path
        )

        if chunk_records_files:
            records_path = SALUSV + '{' + ','.join(chunk_records_files) + '}'

        if self._test:
            # Tests run on local files
            records_path = file_utils.get_abs_path(__file__, records_path) + '/'
            matching_path = file_utils.get_abs_path(__file__, matching_path) + '/'

        records_loader.load_and_clean_all_v2(self._runner, records_path,
                                             records_schemas, load_file_name=True)
        payload_loader.load_all(self._runner, matching_path,
                                matching_payloads_schemas)

    def transform(self, batch_date, batch_id):
        log("Transforming records")
        if self.__class__.__name__ == CensusDriver.__name__:
            census_module = std_census
        else:
            census_module = importlib.import_module(self._base_package)

        # Since this module is in the package, its file path will contain the
        # package path. Remove that in order to find the location of the
        # transformation scripts
        scripts_directory = \
            '/'.join(inspect.getfile(census_module).replace(PACKAGE_PATH, '').split('/')[:-1] + [''])
        content = self._runner.run_all_spark_scripts(variables=[['salt', self._salt]],
                                                     directory_path=scripts_directory)
        if self._no_row_id:
            content = content.drop("rowid")

        return content

    def save(self, dataframe, batch_date, batch_id, chunk_idx=None, header=True):
        log("Saving results to the local file system")
        dataframe.createOrReplaceTempView('deliverable')
        _batch_id_path, _batch_id_value = self._get_batch_info(batch_date, batch_id)
        normalized_records_unloader.unload_delimited_file(
            self._spark, self._runner, SAVE_PATH + '{batch_id_path}/'.format(
                batch_id_path=_batch_id_path
            ),
            'deliverable',
            output_file_name_template=self._output_file_name_template.format(
                batch_id_value=_batch_id_value
            ),
            test=self._test, header=header
        )

    def copy_to_s3(self, batch_date=None, batch_id=None):
        log("Copying files to: " + self._output_path)
        normalized_records_unloader.distcp(self._output_path)

    def stop_spark(self):
        log("Stopping spark.")
        self._spark.stop()
