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
        self.epoch = datetime.datetime.utcfromtimestamp(0)
        self.env = env

    def exectime(self, context):
        if self.env == 'prod':
            return (context['execution_date'] - self.epoch).total_seconds()
        else:
            return (datetime.datetime.utcnow() - self.epoch).total_seconds()

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

            

        api.Event.create(title=title,
                                 text='',
                                 tags=['application:airflow', 'env:'+self.env, 'dag:'+context['dag'].dag_id, 'task:'+task.task_id],
                                 host=ti.hostname,
                                 date_happened = self.exectime(context),
                                 alert_type=type,
                                 aggregation_key=context['run_id'])

    def dd_start(self, context):
        api.Event.create(title='Dag Started: ' + context['dag'].dag_id,
                                 text='',
                                 tags=['application:airflow','env:'+self.env,'dag:'+context['dag'].dag_id],
                                 date_happened = self.exectime(context),
                                 host=context['ti'].hostname,
                                 aggregation_key=context['run_id'])


    def dd_complete(self, context):
        api.Event.create(title='Dag Complete: ' + context['dag'].dag_id,
                                 text='',
                                 tags=['application:airflow','env:'+self.env,'dag:'+context['dag'].dag_id],
                                 host=context['ti'].hostname,
                                 date_happened = self.exectime(context),
                                 alert_type='success',
                                 aggregation_key=context['run_id'])



