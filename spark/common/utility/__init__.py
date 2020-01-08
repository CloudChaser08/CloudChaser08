import json
import urllib.request
from datetime import datetime
from pyspark import SparkContext


class SparkJobNotCompleteException(Exception):
    """Exception for when the history server doesn't think a Spark job is complete"""

def get_spark_runtime(json_endpoint):
    """Returns the total runtime in seconds for all jobs in the current SparkContext.

    Note:
        This will aggregate time from jobs that have succeded, failed, or are still
        running.

    Args:
        json_endpoint (str): A URL that points to Spark's UI Server's endpoint.

    Returns:
        total_runtime (int): The total amount of time it took to run all jobs.
    """

    with urllib.request.urlopen(json_endpoint) as url:
        job_data = json.loads(url.read().decode())

    total_time = 0
    datetime_pattern = "%Y-%m-%dT%H:%M:%S"

    # The time stamps from the SparkWebUI contain extra information relating
    # to timezones that we dont' need, so we slice them out.
    for job in job_data:
        if 'completionTime' not in job:
            raise SparkJobNotCompleteException

        submit_time = \
                datetime.strptime(job['submissionTime'][:19], datetime_pattern)
        completion_time = \
                datetime.strptime(job['completionTime'][:19], datetime_pattern)

        total_time += (completion_time - submit_time).total_seconds()

    return total_time

def format_time(seconds):
    intervals = (
        ('weeks', 604800),
        ('days', 86400),
        ('hours', 3600),
        ('minutes', 60),
        ('seconds', 1),
    )

    result = []

    for name, count in intervals:
        value = int(seconds) // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(value, name))

    return ', '.join(result)


def get_spark_time():
    """Gets the total run time from the active SparkContext.

    This function was created in order to maintain the immutablity of SparkState
    while also working within the confides of Dewey. Ideally, SparkState should
    be used instead of this function.

    Returns:
        total_runtime (int): The total amount of time it took to run all jobs.
    """

    context = SparkContext.getOrCreate()

    conf = context.getConf()

    base_web_url = context.uiWebUrl
    app_name = context.appName.strip()

    app_id = conf.get("spark.app.id")
    history_ui_port = conf.get("spark.history.ui.port")

    url = "{}/api/v1/applications/{}/jobs/".format(base_web_url, app_id)

    return get_spark_runtime(url)
