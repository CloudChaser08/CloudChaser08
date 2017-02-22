from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta

import subdags.s3_validate_file as s3_validate_file

reload(s3_validate_file)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'practice_insight_pipeline'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/practice_insight/'
S3_TRANSACTION_PROCESSED_PATH_TEMPLATE = 's3://salusv/incoming/medicalclaims/practiceinsight/{}/{}/'

TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Practice Insight transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'HV.data.837.{}.csv.gz'  # TODO: CHANGE
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Deid file
DEID_FILE_DESCRIPTION = 'Practice Insight deid file'
DEID_FILE_NAME_TEMPLATE = 'HV.phi.{}.o'  # TODO: CHANGE
MINIMUM_DEID_FILE_SIZE = 500

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 1, 25, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *" if Variable.get(
        "AIRFLOW_ENV", default_var=''
    ).find('prod') != -1 else None,
    default_args=default_args
)


def get_formatted_date(ds, kwargs):
    """
    Just the current month
    """
    return kwargs['ds_nodash'][0:4] + '_' \
        + kwargs['ds_nodash'][4:6]


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))
    return out


def insert_todays_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{6}')
    return out


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['ds_nodash'][0:4],
        kwargs['ds_nodash'][4:6]
    )


def generate_transaction_file_validation_dag(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func': insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]),
                'file_description': 'Practice Insight ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


validate_transactional = generate_transaction_file_validation_dag(
    'transactional', TRANSACTION_FILE_NAME_TEMPLATE,
    10000000
)
validate_deid = generate_transaction_file_validation_dag(
    'deid', DEID_FILE_NAME_TEMPLATE,
    10000000
)
