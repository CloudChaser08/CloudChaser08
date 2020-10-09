import subprocess
from csv import DictWriter
from datetime import datetime
import time
from spark.common.utility.logger import log

from spark.common.utility import get_spark_runtime, format_time, SparkJobNotCompleteException
from spark.common.utility.singleton import Singleton
from spark.common.utility.spark_state import SparkState
from spark.common.utility.run_details import RunDetails


S3_OUTPUT_PATH = "s3://salusv/normalization/logs/"
RETRIES = 5
BACKOFF_INTERVAL = 10 # in seconds


class RunRecorder(object):
    def __init__(self):
        # pylint: disable=no-value-for-parameter
        """Records the details of a particular Spark ingest.

        Attributes:
            spark_state (SparkState): The `SparkState` that was captured during
                the ignest.
            run_details (RunDetails): The `RunDetails` that were captured during
                the ingest.
        """

        self.spark_state = SparkState()
        self.run_details = RunDetails()

    def record_run_details(self, spark_app_runtime=None, additional_time=None):
        """Logs the details of a Spark job to a csv file that's saved on S3.

        Note:
            There is a delay between when the SparkContext stops and when the
            Spark History Server is updated. Therefore, it's possible to get
            a 404 when trying to access it if not enough time as passed.
            The delay in updates can be adjusted with the
            `spark.history.fs.update.interval` config option.

        Args:
            spark_app_runtime (int, optional): The run time of the Spark
                application in seconds. If `None`, then the Spark History
                Server will be accessed in order to determine the run time.
            additional_time (int, optional): The run time of additional operations
                such as Hadoop. If `None`, then only the Spark time will be used
                to represent the total run time.
        """
        log("Recording run details")

        if spark_app_runtime is None:
            retries = RETRIES
            while retries:
                try:
                    spark_runtime = get_spark_runtime(self.spark_state.history_endpoint)
                    break
                except SparkJobNotCompleteException as exc:
                    time.sleep(BACKOFF_INTERVAL)
                    retries -= 1
                    if retries == 0:
                        raise exc
        else:
            spark_runtime = spark_app_runtime

        if additional_time is not None:
            total_time = spark_runtime + additional_time
        else:
            total_time = spark_runtime

        header = [
            'provider_name',
            'data_type',
            'data_source_transaction_path',
            'data_source_matching_path',
            'output_path',
            'run_type',
            'input_date',
            'total_run_time',
            'total_run_time_formatted',
            'date_ran',
            'time_ran'
        ]

        this_date = datetime.today()
        date_formatted = this_date.strftime('%Y-%m-%d')
        time_formatted = this_date.strftime('%H:%M:%S')

        data = {
            'provider_name': self.run_details.provider_name,
            'data_type': self.run_details.data_type.value,
            'data_source_transaction_path': self.run_details.data_source_transaction_path,
            'data_source_matching_path': self.run_details.data_source_matching_path,
            'output_path': self.run_details.output_path,
            'run_type': self.run_details.run_type.value,
            'input_date': self.run_details.input_date,
            'total_run_time': total_time,
            'total_run_time_formatted': format_time(total_time),
            'date_ran': date_formatted,
            'time_ran': time_formatted
        }

        app_name = self.spark_state.app_name
        app_id = self.spark_state.app_id

        file_name = "{name}-{app_id}.csv".format(name=app_name, app_id=app_id).replace(" ", "_")

        local_output_path = \
                "/tmp/{file_name}".format(file_name=file_name)

        with open(local_output_path, 'wt') as f:
            csv_writer = DictWriter(f, fieldnames=header)

            csv_writer.writeheader()
            csv_writer.writerow(data)

        # We have to copy the saved, local csv to hdfs in order to move it
        # to s3 via s3-dist-cp. It might be worth looking into alternative
        # methods as the copying to hdfs and then to s3 takes a bit of time.
        hdfs_path = "hdfs:///"
        try:
            subprocess.check_call(['hdfs',
                                   'dfs',
                                   '-f',
                                   '-copyFromLocal',
                                   local_output_path,
                                   hdfs_path]
                                  )

            subprocess.check_call(['s3-dist-cp',
                                   '--s3ServerSideEncryption',
                                   '--src',
                                   '{hdfs_prefix}{file_name}'.format(
                                       hdfs_prefix=hdfs_path,
                                       file_name=file_name
                                   ),
                                   '--dest',
                                   S3_OUTPUT_PATH + file_name]
                                  )
        except subprocess.CalledProcessError as e:
            print('Failed to log the run.')
            print(e)
