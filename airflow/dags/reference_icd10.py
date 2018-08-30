import common.HVDAG as HVDAG

from datetime import datetime, timedelta
import httplib

from airflow.operators import BranchPythonOperator, PythonOperator, SubDagOperator

import util.hive as hive_utils
import util.date_utils as date_utils

for m in [hive_utils, date_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = 'tmp/reference/icd10/{}'
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

def generate_check_for_file_task(filetype, path_template_function):
    def check_for_http_file(ds, kwargs):
        path = path_template_function(ds, kwargs)
        conn = httplib.HTTPSConnection(ICD_HOSTNAME)
        conn.request('HEAD', path)
        res = conn.getresponse()
        if res.status == '200':
            return 'fetch_{}_file'.format(filetype)
        kwargs.xcom_push(key='head_response', value=res)
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
