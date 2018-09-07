import common.HVDAG as HVDAG

from datetime import datetime, timedelta
import httplib
import logging
import os

from airflow.operators import BranchPythonOperator, DummyOperator, PythonOperator, SubDagOperator

import config as config
import subdags.run_pyspark_routine as run_pyspark_routine
import util.date_utils as date_utils
import util.decompression as decompression
import util.hive as hive_utils
import util.s3_utils as s3_utils
import util.slack as slack

for m in [config, date_utils, decompression,
          hive_utils, s3_utils, slack, HVDAG]:
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
DIAGNOSIS_FILENAME_TEMPLATE = 'icd10cm_order_{}.txt'
DIAGNOSIS_S3_URL_TEMPLATE = 's3://salusv/incoming/reference/icd10/cm/{}/'
DIAGNOSIS_S3_OUTPUT_TEMPLATE = 's3://salusv/reference/parquet/icd10/{}/dx/'
PROCEDURE_ZIP_URL_TEMPLATE = '/Medicare/Coding/ICD10/Downloads/{}-ICD-10-PCS-Order-File.zip'
PROCEDURE_FILENAME_TEMPLATE = 'icd10pcs_order_{}.txt'
PROCEDURE_S3_URL_TEMPLATE = 's3://salusv/incoming/reference/icd10/pcs/{}/'
PROCEDURE_S3_OUTPUT_TEMPLATE = 's3://salusv/reference/parquet/icd10/{}/pcs/'

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

def generate_validate_file_task(filetype, get_tmp_dir_function, filename_template_function,
        *validation_funcs):
    def validate_file(ds, **kwargs):
        tmp_dir = get_tmp_dir_function(ds, kwargs)
        filename = filename_template_function(ds, kwargs)

        with open(tmp_dir + filename, 'r') as f:
            for num, line in enumerate(f):
                for validation_func in validation_funcs:
                    if not validation_func['function'](line):
                        failure_info = {
                            'filename'          : filename,
                            'line_num'          : num + 1,
                            'failure_message'   : validation_func['failure_message']
                        }
                        kwargs['ti'].xcom_push(key='failure_info', value=failure_info)
                        return 'log_{}_format_changed'.format(filetype)
        return 'push_{}_file'.format(filetype)

    return BranchPythonOperator(
        task_id='validate_{}_format'.format(filetype),
        provide_context=True,
        python_callable=validate_file,
        dag=mdag
    )


MAX_CHARACTERS_IN_ROW = 400
ICD10_CM_START_INDEX = 6
validate_cm_format = generate_validate_file_task(
    'cm',
    get_cm_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        DIAGNOSIS_FILENAME_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    ),
    {
        'function': lambda x: len(x) <= MAX_CHARACTERS_IN_ROW,
        'failure_message': 'row is longer than {} characters'.format(MAX_CHARACTERS_IN_ROW)
    },
    {
        'function': lambda x: x[ICD10_CM_START_INDEX].isalpha(),
        'failure_message': 'code does not start with a letter'
    }
)
validate_pcs_format = generate_validate_file_task(
    'pcs',
    get_pcs_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        PROCEDURE_FILENAME_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    ),
    {
        'function': lambda x: len(x) <= MAX_CHARACTERS_IN_ROW,
        'failure_message': 'row is longer than {} characters'.format(MAX_CHARACTERS_IN_ROW)
    }
)

def generate_log_format_changed_task(filetype):
    def log_format_changed(ds, **kwargs):
        res = kwargs['ti'].xcom_pull(task_ids='validate_{}_format'.format(filetype),
                                     key='failure_info'
                                    )
        log_msg = 'Failure for {} file\n'.format(filetype)
        log_msg = log_msg + 'File: {}\n'.format(res['filename'])
        log_msg = log_msg + 'Validation failure on line {}\n'.format(str(res['line_num']))
        log_msg = log_msg + 'Failure message: {}\n'.format(res['failure_message'])
        logging.info(log_msg)
        attachment = {
            'fallback'      : log_msg,
            'color'         : '#ED6504',
            'pretext'       : 'Reference data failure',
            'title'         : 'ICD10 Reference Data Validation Failed',
            'title_link'    : kwargs['ti'].log_url,
            'text'          : log_msg
        }
        slack.send_message(config.AIRFLOW_ALERTS_CHANNEL, attachment=attachment)
        
    return PythonOperator(
        task_id='log_{}_format_changed'.format(filetype),
        provide_context=True,
        python_callable=log_format_changed,
        dag=mdag
    )


log_cm_format_changed = generate_log_format_changed_task('cm')
log_pcs_format_changed = generate_log_format_changed_task('pcs')

def generate_push_file_task(filetype, get_tmp_dir_function, filename_template_function,
        s3_location_template_function):
    def push_file(ds, **kwargs):
        tmp_dir = get_tmp_dir_function(ds, kwargs)
        filename = filename_template_function(ds, kwargs)
        s3_location = s3_location_template_function(ds, kwargs)

        s3_utils.copy_file(tmp_dir + filename, s3_location)

    return PythonOperator(
        task_id='push_{}_file'.format(filetype),
        provide_context=True,
        python_callable=push_file,
        dag=mdag
    )


push_cm_file = generate_push_file_task(
    'cm',
    get_cm_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        DIAGNOSIS_FILENAME_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    ),
    date_utils.generate_insert_date_into_template_function(
        DIAGNOSIS_S3_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)
push_pcs_file = generate_push_file_task(
    'pcs',
    get_pcs_tmp_dir,
    date_utils.generate_insert_date_into_template_function(
        PROCEDURE_FILENAME_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    ),
    date_utils.generate_insert_date_into_template_function(
        PROCEDURE_S3_URL_TEMPLATE,
        year_offset=ICD10_YEAR_OFFSET
    )
)

def norm_args(ds, kwargs):
    base = ['--year', date_utils.insert_date_into_template({}, kwargs, year_offset=ICD10_YEAR_OFFSET)]
    return base


transform_to_parquet = SubDagOperator(
    subdag=run_pyspark_routine.run_pyspark_routine(
        DAG_NAME,
        'run_transform_to_parquet',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'EMR_CLUSTER_NAME_FUNC' : date_utils.generate_insert_date_into_template_function(
                'reference_icd10_{}',
                year_offset=ICD10_YEAR_OFFSET
            ),
            'PYSPARK_SCRIPT_NAME'   : '/home/hadoop/spark/reference/icd10/sparkTransformToParquet.py',
            'PYSPARK_ARGS_FUNC'     : norm_args,
            'NUM_NODES'             : '2',
            'NODE_TYPE'             : 'm4.xlarge',
            'EBS_VOLUME_SIZE'       : '50',
            'PURPOSE'               : 'reference_data_update',
            'CONNECT_TO_METASTORE'  : False
        }
    ),
    task_id='transform_to_parquet',
    dag=mdag
)

def do_update_table_locations(ds, **kwargs):
    new_diagnosis_loc = date_utils.insert_date_into_template(DIAGNOSIS_S3_OUTPUT_TEMPLATE, kwargs, year_offset=ICD10_YEAR_OFFSET)
    new_procedure_loc = date_utils.insert_date_into_template(PROCEDURE_S3_OUTPUT_TEMPLATE, kwargs, year_offset=ICD10_YEAR_OFFSET)
    queries = [
        'ALTER TABLE ref_icd10_diagnosis SET LOCATION {}'.format(new_diagnosis_loc),
        'ALTER TABLE ref_icd10_procedure SET LOCATION {}'.format(new_procedure_loc)
    ]
    hive_utils.hive_execute(queries)


update_table_locations = PythonOperator(
    task_id='update_table_locations',
    provide_context=True,
    python_callable=do_update_table_locations,
    dag=mdag
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
