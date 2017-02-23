from datadog import initialize, api
import datetime

from airflow.operators.dummy_operator import DummyOperator

def start_dag_op(dag, dd):
    return DummyOperator(
        task_id = 'start_dag',
        on_success_callback = dd.dd_start,
        dag=dag)

def end_dag_op(dag, dd):
    return DummyOperator(
        task_id = 'end_dag',
        on_success_callback = dd.dd_complete,
        dag=dag
    )


class hv_datadog():
    def __init__(self, env, keys):
        initialize(**keys)
        self.env = env

    def dd_eventer(self, context):
        task   = context['task']
        ti     = context['ti']
        title  = "Task"
        type   = 'info'
        suffix = ''
        if ti.state == 'success':
            type  = 'success'
            suffix = 'Succeeded'
        elif ti.state == 'failed':
            type   = 'error'
            suffix = 'Failed'
        elif ti.state == 'up_for_retry':
            type   = 'warning'
            suffix = 'Retrying'

        title = 'Task ' + suffix + ': ' + task.task_id

        tags=['application:airflow',
              'env:'+self.env,
              'dag:'+context['dag'].dag_id,
              'dag_ds:'+context['ds'],
              'task:'+task.task_id]

        api.Event.create(title=title,
                                 text="%%% \n**Command:** `" + ti.command() + "`\n\n**Log:** [" + ti.log_url + "](" + ti.log_url + ")\n %%%",
                                 tags=tags,
                                 host=ti.hostname,
                                 alert_type=type,
                                 aggregation_key=context['run_id'])

        api.Metric.send(metric='airflow.task.'+ti.state, points=1, host=ti.hostname, tags=tags)

    def dd_start(self, context):
        tags=['application:airflow',
              'env:'+self.env,
              'dag:'+context['dag'].dag_id,
              'dag_ds:'+context['ds']]

        api.Event.create(title='Dag Started: ' + context['dag'].dag_id,
                                 text='',
                                 tags=tags,
                                 host=context['ti'].hostname,
                                 aggregation_key=context['run_id'])

        api.Metric.send(metric='airflow.dag.started', points=1, host=context['ti'].hostname, tags=tags)


    def dd_complete(self, context):
        tags=['application:airflow',
              'env:'+self.env,
              'dag:'+context['dag'].dag_id,
              'dag_ds:'+context['ds']]

        api.Event.create(title='Dag Complete: ' + context['dag'].dag_id,
                                 text='',
                                 tags=tags,
                                 host=context['ti'].hostname,
                                 alert_type='success',
                                 aggregation_key=context['run_id'])

        api.Metric.send(metric='airflow.dag.ended', points=1, host=context['ti'].hostname, tags=tags)


