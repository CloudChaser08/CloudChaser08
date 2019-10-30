import json
import urllib.request
from datetime import datetime


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
