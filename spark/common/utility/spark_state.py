from pyspark import SparkContext

from spark.common.utility.singleton import Singleton


class SparkState(metaclass=Singleton):
    __slots__ = ['ui_url', 'app_name', 'app_id', 'history_ui_port']

    def __init__(self, ui_url, app_name, app_id, history_ui_port):
        """Represents details regarding the Spark state during an ingest.

        Args:
            ui_url (str): The URL of the Spark web UI server.
            app_name (str): The name of the Saprk application.
            app_id (str): The ID of the Spark application.
            history_ui_port (str): The port of the Spark's history server.

        Attributes:
            ui_url (str): The URL of the Spark web UI server.
            app_name (str): The name of the Saprk application.
            app_id (str): The ID of the Spark application.
            history_ui_port (str): The port of the Spark's history server.
        """

        self.ui_url = ui_url
        self.app_name = app_name
        self.app_id = app_id
        self.history_ui_port = history_ui_port

    @classmethod
    def get_current_state(cls):
        """Creates a `SparkState` by accessing a running `SparkContext`.

        Returns:
            `SparkState`
        """

        context = SparkContext.getOrCreate()

        conf = context.getConf()
        base_web_url = context.uiWebUrl
        app_name = context.appName.strip()

        app_id = conf.get("spark.app.id")
        history_ui_port = conf.get("spark.history.ui.port")

        return cls(base_web_url, app_name, app_id, history_ui_port)

    @property
    def active_endpoint(self):
        """The REST endpoint of the current running Spark application.

        Returns:
            str
        """

        url = "{base_url}/api/v1/applications/{id}/jobs/"\
                .format(base_url=self.ui_url, id=self.app_id)

        return url

    @property
    def history_endpoint(self):
        """The REST endpoint of the Spark history server.

        Returns:
            str
        """

        base_url = ':'.join(self.ui_url.split(':', 2)[:2])

        url = "{base_url}:{port}/api/v1/applications/{id}/jobs"\
                .format(base_url=base_url, port=self.history_ui_port, id=self.app_id)

        return url
