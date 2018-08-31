import common.HVDAG as HVDAG

from datetime import datetime, timedelta
import httplib
import logging
import os

from airflow.operators import BranchPythonOperator, DummyOperator, PythonOperator, SubDagOperator

import config as config
import util.date_utils as date_utils
import util.decompression as decompression
import util.hive as hive_utils
import util.slack as slack

for m in [config, date_utils, decompression,
          hive_utils, slack, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/reference/icd10/{}/'
DAG_NAME = 'reference_icd10'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 10, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = None,       #TODO: determine later
    default_args = default_args
)

ICD_HOSTNAME = 'www.cms.gov'
DIAGNOSIS_ZIP_URL_TEMPLATE = '/Medicare/Coding/ICD10/Downloads/{}-ICD-10-CM-Code-Descriptions.zip'
PROCEDURE_ZIP_URL_TEMPLATE = '/Medicare/Coding/ICD10/Downloads/{}-ICD-10-PCS-Order-File.zip'

ICD10_YEAR_OFFSET = 2

get_cm_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'cm/',
    year_offset=ICD10_YEAR_OFFSET
)
get_pcs_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'pcs/',
    year_offset=ICD10_YEAR_OFFSET
)

def generate_check_for_file_task(filetype, path_template_function):
    def check_for_http_file(ds, **kwargs):
        path = path_template_function(ds, kwargs)
        conn = httplib.HTTPSConnection(ICD_HOSTNAME)
        conn.request('HEAD', path)
        res = conn.getresponse()
        if res.status == 200:
            return 'fetch_{}_file'.format(filetype)
        response_info = {
            'url'       : ICD_HOSTNAME + path,
            'status'    : res.status,
            'reason'    : res.reason,
            'headers'   : res.getheaders()
        }
        kwargs['ti'].xcom_push(key='head_response', value=response_info)
        return 'log_{}_file_not_found'.format(filetype)

    return BranchPythonOperator(
        task_id='check_{}_file'.format(filetype),
        provide_context=True,
        python_callable=check_for_http_file,
        dag=mdag
    )

check_for_cm_file = generate_check_for_file_task(
    'cm',
    date_utils.generate_insert_date_into_template_function(
        DIAGNOSIS_ZIP_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)
check_for_pcs_file = generate_check_for_file_task(
    'pcs',
    date_utils.generate_insert_date_into_template_function(
        PROCEDURE_ZIP_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)

def generate_log_file_not_found_task(filetype):
    def log_file_not_found(ds, **kwargs):
        res = kwargs['ti'].xcom_pull(task_ids='check_{}_file'.format(filetype),
                                     key='head_response'
                                    )
        log_msg = 'Head response for {}\n'.format(res['url'])
        log_msg = log_msg + str(res['status']) + '\t' + res['reason'] + '\n'
        for header in res['headers']:
            log_msg = log_msg + header[0] + ': ' + header[1] + '\n'
        logging.info(log_msg)
        attachment = {
            'fallback'      : log_msg,
            'color'         : '#ED6504',
            'pretext'       : 'Reference data failure',
            'title'         : 'ICD10 Reference Data Not Found',
            'title_link'    : kwargs['ti'].log_url,
            'text'          : log_msg
        }
        slack.send_message(config.AIRFLOW_ALERTS_CHANNEL, attachment=attachment)
        
    return PythonOperator(
        task_id='log_{}_file_not_found'.format(filetype),
        provide_context=True,
        python_callable=log_file_not_found,
        dag=mdag
    )


log_cm_file_not_found = generate_log_file_not_found_task('cm')
log_pcs_file_not_found = generate_log_file_not_found_task('pcs')

def generate_fetch_file_task(filetype, get_tmp_dir_function, path_template_function):
    def fetch_file(ds, **kwargs):
        tmp_dir = get_tmp_dir_function(ds, kwargs)
        path = path_template_function(ds, kwargs)

        os.makedirs(tmp_dir)

        conn = httplib.HTTPSConnection(ICD_HOSTNAME)
        conn.request('GET', path)
        res = conn.getresponse()

        output_file = tmp_dir + path.split('/')[-1]
        with open(output_file, 'wb') as f:
            f.write(res.read())

        decompression.decompress_zip_file(output_file, tmp_dir)

    return PythonOperator(
        task_id='fetch_{}_file'.format(filetype),
        provide_context=True,
        python_callable=fetch_file,
        dag=mdag
    )


fetch_cm_file = generate_fetch_file_task(
    'cm',
    get_cm_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        DIAGNOSIS_ZIP_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)
fetch_pcs_file = generate_fetch_file_task(
    'pcs',
    get_pcs_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        PROCEDURE_ZIP_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)

validate_cm_format = DummyOperator(task_id='validate_cm_format', dag=mdag)
validate_pcs_format = DummyOperator(task_id='validate_pcs_format', dag=mdag)
log_cm_format_changed = DummyOperator(task_id='log_cm_format_changed', dag=mdag)
log_pcs_format_changed = DummyOperator(task_id='log_pcs_format_changed', dag=mdag)
push_cm_file = DummyOperator(task_id='push_cm_file', dag=mdag)
push_pcs_file = DummyOperator(task_id='push_pcs_file', dag=mdag)
transform_to_parquet = DummyOperator(task_id='transform_to_parquet', dag=mdag)
update_table_locations = DummyOperator(task_id='update_table_locations', dag=mdag)

### DAG Structure ###
fetch_cm_file.set_upstream(check_for_cm_file)
log_cm_file_not_found.set_upstream(check_for_cm_file)

fetch_pcs_file.set_upstream(check_for_pcs_file)
log_pcs_file_not_found.set_upstream(check_for_pcs_file)

validate_cm_format.set_upstream(fetch_cm_file)
validate_pcs_format.set_upstream(fetch_pcs_file)

push_cm_file.set_upstream(validate_cm_format)
log_cm_format_changed.set_upstream(validate_cm_format)

push_pcs_file.set_upstream(validate_pcs_format)
log_pcs_format_changed.set_upstream(validate_pcs_format)

transform_to_parquet.set_upstream([push_cm_file, push_pcs_file])

update_table_locations.set_upstream(transform_to_parquet)
