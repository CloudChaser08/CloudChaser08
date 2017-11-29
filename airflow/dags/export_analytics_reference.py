import common.HVDAG as HVDAG

from airflow.operators import BashOperator, PythonOperator

from airflow.models import Variable

from datetime import timedelta, datetime
from time import gmtime, mktime
import os
import util.hive
from subprocess import check_output

for m in [HVDAG, util.hive]:
    reload(m)

from util.hive import hive_execute

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = HVDAG.HVDAG(
    'export_analytics_reference',
    default_args=default_args,
    start_date=datetime(2017, 11, 20),
    schedule_interval='@daily'
)
psql_env                = dict(os.environ)
psql_env['PGUSER']      = 'analytics'
psql_env['PGPASSWORD']  = Variable.get('ANALYTICS_REFERENCE_PASSWORD')
psql_env['PGDATABASE']  = 'reference'
psql_env['PGPORT']      = '5432'
psql_env['PGHOST']      = 'reference.aws.healthverity.com'

def generate_table_export_task(table, s3_path):
    COMMAND = """psql -c "\copy (SELECT * FROM {table}) TO STDOUT WITH CSV DELIMITER '|';" | lbzip2 -c | aws s3 cp - {s3_path}"""

    return BashOperator(
        task_id='export_{}'.format(table),
        env=psql_env,
        bash_command=COMMAND.format(table=table, s3_path=s3_path),
        dag=dag
    )

def psql_to_hive_type(psql_type):
    psql_hive_type = {
        'character' : 'string',
        'integer'   : 'int',
        'date'      : 'date'
    }

    # We want to map multiple Postgres types to a hive type
    # (e.g. character(10) => string and character varying(256) => string)
    # This loop converts from a Postgres type of a hive type based on a
    # substring match
    for k in psql_hive_type:
        if k in psql_type:
            return psql_hive_type[k]

    raise ValueError("Unexpected psql type")

CREATE_CSV_TABLE_TEMPLATE = """
CREATE EXTERNAL TABLE dw_stg.{table}_csv (
    {table_structure}
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
STORED AS TEXTFILE
LOCATION '{s3a_url}'
"""

CREATE_PARQUET_TABLE_TEMPLATE = """
CREATE EXTERNAL TABLE dw_stg.{table}_{ts} (
    {table_structure}
)
STORED AS PARQUET
LOCATION '{s3a_url}'
"""

INSERT_INTO_PARQUET_TABLE_TEMPLATE = """
INSERT INTO dw_stg.{table}_{ts}
SELECT 
    {nullified_columns}
FROM dw_stg.{table}_csv
DISTRIBUTE BY rand()
"""

DROP_TABLE_TEMPLATE = """
DROP TABLE IF EXISTS {}
"""

RENAME_TABLE_TEMPLATE = """
ALTER TABLE {} RENAME TO {}
"""

SELECT_SCHEMA_TEMPLATE = """
SELECT column_name, data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{}'
"""

NULLIFY_COLUMN_TEMPLATE = """
CASE WHEN {column} = '' THEN NULL ELSE {column} END AS {column}
"""

def generate_table_create_task(table, csv_s3a_url, parquet_s3a_url):
    def do_table_create(ds, **kwargs):
        ts = int(mktime(gmtime()))
        p_s3a_url = parquet_s3a_url.format(ts=ts)

        # Generate the Hive table structure from the PSQL table structure
        table_struct_raw   = check_output('psql -c "\d {}"'.format(table), env=psql_env, shell=True)
        column_details_raw = [l for l in table_struct_raw.split('\n') if '|' in l][1:]
        column_details     = [[v.strip() for v in c.split('|')[:2]] for c in column_details_raw]
        columns_hive       = [[c[0], psql_to_hive_type(c[1])] for c in column_details]
        table_structure    = ','.join([' '.join(c) for c in columns_hive])

        create_csv_table            = CREATE_CSV_TABLE_TEMPLATE.format(
                table=table, table_structure=table_structure, s3a_url=csv_s3a_url
        )
        create_parquet_table        = CREATE_PARQUET_TABLE_TEMPLATE.format(
                table=table, table_structure=table_structure, s3a_url=p_s3a_url, ts=ts
        )

        nullified_columns = ','.join([NULLIFY_COLUMN_TEMPLATE.format(column=c[0]) for c in columns_hive])
        insert_into_parquet_table = INSERT_INTO_PARQUET_TABLE_TEMPLATE.format(table=table, nullified_columns=nullified_columns, ts=ts)
        sqls = [
            DROP_TABLE_TEMPLATE.format('dw_stg.' + table + '_csv'), # Just to be safe
            DROP_TABLE_TEMPLATE.format('dw_stg.' + table + '_' + str(ts)), # Just to be safe
            create_csv_table,
            create_parquet_table,
            'SET spark.sql.shuffle.partitions=5',
            'SET parquet.compression=GZIP',
            insert_into_parquet_table,
            DROP_TABLE_TEMPLATE.format('dw.' + table),
            RENAME_TABLE_TEMPLATE.format('dw_stg.' + table + '_' + str(ts), 'dw.' + table),
            DROP_TABLE_TEMPLATE.format('dw_stg.' + table + '_csv'),
        ]
        hive_execute(sqls)

    return PythonOperator(
        task_id='create_{}'.format(table),
        provide_context=True,
        python_callable=do_table_create,
        dag=dag
    )

S3_PATH_TEMPLATE = '{proto}://salusv/reference/{format}/dw/{table}'
tables_to_export = ['gen_ref', 'gen_ref_dt', 'gen_ref_whtlst', 'ref_geo_state',
        'ref_vdr', 'ref_vdr_feed']

if HVDAG.HVDAG.airflow_env != 'test':
    for t in tables_to_export:
        export_task = generate_table_export_task(t,
                S3_PATH_TEMPLATE.format(proto='s3', format='text', table=t) + '/{}.bz2'.format(t))
        create_task = generate_table_create_task(t,
                S3_PATH_TEMPLATE.format(proto='s3a', format='text', table=t) + '/',
                S3_PATH_TEMPLATE.format(proto='s3a', format='parquet', table=t) + '_{ts}/')
        create_task.set_upstream(export_task)
if HVDAG.HVDAG.airflow_env == 'test':
    generate_table_export_task('gen_ref', 's3://salusv/testing/dewey/airflow/e2e/export_analytics_reference/gen_ref.bz2')
