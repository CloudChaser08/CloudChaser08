import json
import datadog
from airflow.models import Variable

class Datadog():
    def __init__(self):
        datadog.initialize(
            **json.loads(Variable.get('DATADOG_KEYS'))
        )

    def create_metric(self, name, value, tags):
        """
        Create a metric in datadog. The host for this metric will be the current airflow server.
        """
        datadog.api.Metric.send(metric=name, points=value, tags=tags)
