from airflow.models import DagBag, DagRun
from airflow.utils.state import State
import sys
from datetime import datetime, timedelta
from airflow import settings

dagbag = DagBag()
dag_id = sys.argv[1]
dag = dagbag.get_dag(dag_id)
exec_date = datetime.strptime(sys.argv[2], '%Y-%m-%dT%H%M%S')

# Trick to figure out when the exact execution datetime should be
# if a dag is hourly and triggers at a time other than the top of the hour
# the range will contain 2 execution dates
rng = dag.date_range(exec_date, end_date=exec_date + timedelta(minutes=59))
if len(rng) == 2:
    exec_datetime = rng[-1]
else:
    # if a dag rans no more than once a day, and triggers at a time other than
    # the beginning of the day, the range will contain 2 execution dates
    # if it triggers at the beginning of the day, the range will contain 1
    # execution day
    rng = dag.date_range(exec_date, end_date=exec_date + timedelta(hours=23))
    exec_datetime = rng[-1]

run_id = datetime.strftime(exec_datetime, 'trig__%Y-%m-%dT%H:%M:%S')

# Similar to how TriggerDagRunOperator works
# https://github.com/apache/incubator-airflow/blob/1.7.1.3/airflow/operators/dagrun_operator.py
session = settings.Session()
dr = DagRun(
    dag_id           = dag_id,
    run_id           = run_id,
    execution_date   = exec_datetime,
    # external_trigger must be set to False. If True and the DAG runs on a
    # schedule, the Airflow scheduler will trigger the dag again and throw
    # errors because it will see that the execution date already exsists
    # in the database
    external_trigger = False
)
session.add(dr)
session.commit()
session.close()

