import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import json
from airflow.operators import BashOperator, PythonOperator, SubDagOperator
from airflow.models import Variable
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import subdags.s3_fetch_file as s3_fetch_file
import util.date_utils as date_utils
import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils

for m in [date_utils, s3_fetch_file, s3_utils, sftp_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/consent/mckesson/{}{}{}/'
FILE_NAME_TEMPLATE = 'hv_aimovig_mckesson_consent_update_{}{}{}.txt.gz'

if HVDAG.HVDAG.airflow_env == 'test':
    EXPORT_S3_PATH = 's3://healthverityconsent/outgoing/testing/'
    MIDDLEWARE_CALL = 'http://consent-amgen.aws.healthverity.com:8080/export/testing'
else:
    EXPORT_S3_PATH = 's3://healthverityconsent/outgoing/1234/'
    MIDDLEWARE_CALL = 'http://consent-amgen.aws.healthverity.com:8080/export/1234'

DAG_NAME = 'consent_mckesson_extract'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 8, 28, 15, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    'consent_mckesson_extract',
    default_args=default_args,
    schedule_interval='30 15 * * *',  # run daily at 11:30am ET
)

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

create_tmp_files = BashOperator(
    task_id='create_tmp_dir',
    bash_command='mkdir -p ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') + ';'
    'touch ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') +
    FILE_NAME_TEMPLATE.format('{{ ds_nodash }}', '', '') + ';',
    dag=mdag
)


run_export = BashOperator(
    task_id='run_export',
    bash_command='curl -X post ' + MIDDLEWARE_CALL,
    dag=mdag
)


fetch_file_from_s3 = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_file_from_s3',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                FILE_NAME_TEMPLATE
            ),
            'regex_name_match'        : True,
            's3_prefix'              : '/'.join(EXPORT_S3_PATH.split('/')[3:]),
            's3_bucket'              : 'healthverityconsent'
        }
    ),
    task_id='fetch_file_from_s3',
    dag=mdag
)


def do_deliver_extracted_data(ds, **kwargs):
    if HVDAG.HVDAG.airflow_env == 'test':
        sftp_config = json.loads(Variable.get('consent_mckesson_test_sftp_configuration'))
    else:
        sftp_config = json.loads(Variable.get('consent_mckesson_prod_sftp_configuration'))
    path = sftp_config['path']

    sftp_utils.upload_file(
        get_tmp_dir(ds, kwargs) + date_utils.insert_date_into_template(FILE_NAME_TEMPLATE, kwargs),
        ignore_host_key=True,
         **sftp_config
    )


deliver_extracted_data = PythonOperator(
    task_id='deliver_extracted_data',
    provide_context=True,
    python_callable=do_deliver_extracted_data,
    dag=mdag
)

clean_up_workspace = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        DAG_NAME,
        'clean_up_workspace',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE
        }
    ),
    task_id='clean_up_workspace',
    dag=mdag
)

fetch_file_from_s3.set_upstream(run_export)
fetch_file_from_s3.set_upstream(create_tmp_files)
deliver_extracted_data.set_upstream(fetch_file_from_s3)
clean_up_workspace.set_upstream(deliver_extracted_data)
