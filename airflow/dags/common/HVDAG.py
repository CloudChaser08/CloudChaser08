from airflow.models import Variable, DagBag
from airflow import DAG
import util.slack as slack
import config as config

for m in [config, slack]:
    reload(m)

class HVDAG(DAG):
    airflow_env = {
        'prod' : 'prod',
        'test' : 'test',
        'dev'  : 'dev'
    }[Variable.get("AIRFLOW_ENV", default_var='dev')]

    def __init__(self, dag_id, default_args={}, **kwargs):
        kwargs['dag_id'] = dag_id
        if 'on_failure_callback' not in default_args and self.airflow_env == 'prod':
            default_args['on_failure_callback'] = self._on_failure
        if 'on_retry_callback' not in default_args and self.airflow_env == 'prod':
            default_args['on_retry_callback'] = self._on_retry
        kwargs['default_args'] = default_args

        if 'schedule_interval' in kwargs and self.airflow_env not in ['prod', 'test']:
            kwargs['schedule_interval'] = None

        return super(HVDAG, self).__init__(**kwargs)

    def _on_failure(self, context):
        ti = context['task_instance']
        message = "Failed task\nDAG: {}\nTASK: {}\nEXECUTION DATE: {}\nFAILURE TIMESTAMP:{}\n<{}|Task log>".format(
            ti.dag_id, ti.task_id, ti.execution_date, ti.start_date, ti.log_url
        )
        attachment = {
            "fallback"   : message,
            "color"      : '#D50200',
            "pretext"    : "Airflow task failure",
            "title"      : 'DAG "{}", Task "{}" FAILED!'.format(ti.dag_id, ti.task_id),
            "title_link" : ti.log_url,
            "text"       : "Execution date: {}\nFailure timestamp {}".format(ti.execution_date, ti.start_date)
        }

        slack.send_message(config.SLACK_CHANNEL, attachment=attachment)

    def _on_retry(self, context):
        """Clears a subdag's tasks on retry.
            based on https://gist.github.com/nathairtras/6ce0b0294be8c27d672e2ad52e8f2117"""
        dag_id = "{}.{}".format(
            context['dag'].dag_id,
            context['ti'].task_id,
        )
        execution_date = context['execution_date']
        sdag = DagBag().get_dag(dag_id)
        sdag.clear(
            start_date=execution_date,
            end_date=execution_date,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=False)
