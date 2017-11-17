import common.HVDAG as HVDAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

from datetime import timedelta, datetime
import os

for m in [HVDAG]:
    reload(m)

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = HVDAG.HVDAG(
    'export_analytics_reference',
    default_args=default_args,
    start_date=datetime(2017, 11, 16),
    schedule_interval=None
)

DB_HOST = 'reference.aws.healthverity.com'
DB_USER = 'analytics'
DB_PASSWORD = Variable.get('ANALYTICS_REFERENCE_PASSWORD')
DB_NAME = 'reference'
DB_PORT = '5432'
COMMAND = """psql -c "\copy (SELECT * FROM {table}) TO STDOUT WITH CSV DELIMITER '|';" | lbzip2 -c | aws s3 cp - {s3_path}"""

def generate_table_export_task(table, s3_path):
    env = dict(os.environ)
    env['PGUSER']     = DB_USER
    env['PGPASSWORD'] = DB_PASSWORD
    env['PGDATABASE'] = DB_NAME
    env['PGPORT']     = DB_PORT
    env['PGHOST']     = DB_HOST

    return BashOperator(
        task_id='export_{}'.format(table),
        env=env,
        bash_command=COMMAND.format(table=table, s3_path=s3_path),
        dag=dag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    generate_table_export_task('gen_ref', 's3://salusv/reference/dw/gen_ref/gen_ref.bz2')
    generate_table_export_task('gen_ref_dt', 's3://salusv/reference/dw/gen_ref_dt/gen_ref_dt.bz2')
    generate_table_export_task('gen_ref_whtlst', 's3://salusv/reference/dw/gen_ref_whtlst/gen_ref_whtlst.bz2')
    generate_table_export_task('gen_geo_state', 's3://salusv/reference/dw/gen_geo_state/gen_geo_state.bz2')
    generate_table_export_task('gen_vdr', 's3://salusv/reference/dw/gen_vdr/gen_vdr.bz2')
    generate_table_export_task('gen_vdr_feed', 's3://salusv/reference/dw/gen_vdr_feed/gen_vdr_feed.bz2')
if HVDAG.HVDAG.airflow_env == 'test':
    generate_table_export_task('gen_ref', 's3://salusv/testing/dewey/airflow/e2e/export_analytics_reference/gen_ref.bz2')
