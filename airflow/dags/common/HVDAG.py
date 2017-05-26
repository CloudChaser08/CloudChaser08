from airflow import DAG
import util.slack as slack
import config as config

for m in [config, slack]:
    reload(m)

class HVDAG(DAG):
    def __init__(self, dag_id, default_args={}, **kwargs):
        kwargs['dag_id'] = dag_id
        if 'on_failure_callback' not in default_args:
            default_args['on_failure_callback'] = self._on_failure
        kwargs['default_args'] = default_args
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
