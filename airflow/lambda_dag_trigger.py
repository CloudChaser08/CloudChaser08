from airflow.models import DagBag, DagRun
from airflow.utils.state import State
import sys
from datetime import datetime, timedelta
from airflow import settings

dagbag = DagBag()
dag_id = sys.argv[1]
dag = dagbag.get_dag(dag_id)
exec_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')

# Trick to figure out when the exact execution datetime should be
exec_datetime = dag.date_range(exec_date, end_date=exec_date + timedelta(days=1))[-1]

run_id = datetime.strftime(exec_datetime, 'trig__%Y-%m-%dT%H:%M:%S')

# Similar to how TriggerDagRunOperator works
# https://github.com/apache/incubator-airflow/blob/1.7.1.3/airflow/operators/dagrun_operator.py
session = settings.Session()
dr = DagRun(
    dag_id           = dag_id,
    run_id           = run_id,
    execution_date   = exec_datetime,
    external_trigger = True
)
session.add(dr)
session.commit()
session.close()

