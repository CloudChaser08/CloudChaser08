from datetime import datetime, timedelta
import os
import re

from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.s3_push_files as s3_push_files
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import util.decompression as decompression
import util.s3_utils as s3_utils
import util.date_utils as date_utils
import util.hive as hive

for m in [s3_validate_file, s3_fetch_file, s3_push_files,
          HVDAG, decompression, s3_utils,
          clean_up_tmp_dir, date_utils, hive]:
    reload(m)

DAG_NAME = 'aln441_pre_delivery_staging'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 28, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * 3",
    default_args=default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    S3_DATA_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/aln_441_pre_delivery_staging/data/'
    S3_DATA_STAGED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/aln_441_pre_delivery_staging/staged/'
else:
    S3_DATA_RAW_URL = 's3://healthverity/incoming/quest/'
    S3_DATA_STAGED_URL_TEMPLATE = 's3://salusv/incoming/staging/aln441/data/{}/{}/{}/'

TMP_PATH_TEMPLATE='/tmp/aln441/pre_staging/{}{}{}/'
DATA_FILE_NAME_TEMPLATE = 'HVRequest_output_000441_{}{}{}\d{{6}}.txt.zip'
DECOMPRESSED_DATA_FILE_NAME_TEMPLATE = 'HVRequest_output_000441_{}{}{}\d{{6}}.txt$'
ALN441_PRE_DELIVERY_DAY_OFFSET = 7

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

def get_files_matching_template(template, ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    file_regex = date_utils.insert_date_into_template(template, kwargs, day_offset = ALN441_PRE_DELIVERY_DAY_OFFSET)
    return [file_dir + f for f in os.listdir(file_dir) if re.search(file_regex, f)]


def do_check_for_file(ds, **kwargs):
    days_to_check = kwargs['days_to_check']

    s3_keys = s3_utils.list_s3_bucket_files(
        's3://' + kwargs['s3_bucket'] + '/' + kwargs['s3_prefix'] + '/'
    )

    file_found = False
    for i in range(days_to_check):
        d = (kwargs['execution_date'] + timedelta(days=i)).strftime('%Y-%m-%d')
        expected_file_name = kwargs['expected_filename_func'](d, kwargs)
        found_results = filter(lambda k: re.search(expected_file_name, k.split('/')[-1], s3_keys))
        if len(found_results) > 0:
            file_found = True
            kwargs['ti'].xcom_push(key = 'filename', value = found_results[0])
            break

    if not file_found:
        raise Exception('No file has been found')


check_for_files = PythonOperator(
    task_id='check_for_files',
    provide_context=True,
    python_callable=do_check_for_files,
    op_kwargs={
        's3_prefix'                 : '/'.join(S3_DATA_RAW_URL.split('/')[3:]),
        's3_bucket'                 : S3_DATA_RAW_URL.split('/')[2],
        'expected_filename_func'    : date_utils.generate_insert_date_into_template(
                                        DATA_FILE_NAME_TEMPLATE,
                                        day_offset = ALN441_PRE_DELIVERY_DAY_OFFSET
                                      )
    }
    dag=mdag
)

fetch_data_file_dag = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_data_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func':
                date_utils.generate_insert_date_into_template_function(
                    DATA_FILE_NAME_TEMPLATE,
                    day_offset = ALN441_PRE_DELIVERY_DAY_OFFSET
            ),
            's3_prefix'              : '/'.join(S3_DATA_RAW_URL.split('/')[3:]),
            's3_bucket'              : S3_DATA_RAW_URL.split('/')[2],
            'regex_name_match'       : True
        }
    ),
    task_id='fetch_data_file',
    dag=mdag
)

def do_unzip_file(ds, **kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    files = get_files_matching_template(DATA_FILE_NAME_TEMPLATE, ds, kwargs)
    decompression.decompress_zip_file(files[0], file_dir)


unzip_data_file = PythonOperator(
    task_id='unzip_data_file',
    provide_context=True,
    python_callable=do_unzip_file,
    dag=mdag
)

def do_get_filename(ds, **kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    files = get_files_matching_template(DECOMPRESSED_DATA_FILE_NAME_TEMPLATE, ds, kwargs)
    kwargs['ti'].xcom_push(key='filename', value=files[0])


get_filename = PythonOperator(
    task_id = 'get_filename',
    provide_context = True,
    python_callable = do_get_filename,
    dag = mdag
)

push_file_dag = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_data_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'file_paths_func'   : lambda ds, k: [k['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_filename', key='filename')],
            's3_prefix_func'    : lambda ds, k: '/'.join(S3_DATA_STAGED_URL_TEMPLATE.split('/')[3:]),
            's3_bucket'         : S3_DATA_STAGED_URL_TEMPLATE.split('/')[2]
        }
    ),
    task_id='push_data_file',
    dag=mdag
)

def do_create_table(ds, **kwargs):
    create_table_statement_template = '''
    CREATE EXTERNAL TABLE {}.{} (
        ACCN_ID                 string,
        DOS                     string,
        DOS_ID                  string,
        LAB_CODE                string,
        QBS_PAYOR_CD            string,
        INSURANCE_BILLING_TYPE  string,
        LOCAL_PROFILE_CODE      string,
        STANDARD_PROFILE_CODE   string,
        PROFILE_NAME            string,
        LOCAL_ORDER_CODE        string,
        STANDARD_ORDER_CODE     string,
        ORDER_NAME              string,
        HIPAA_ZIP               string,
        HIPAA_DOB               string,
        HIPAA_AGE               string,
        GENDER                  string,
        ACCT_ID                 string,
        ACCT_NAME               string,
        ACCT_ADDRESS_1          string,
        ACCT_ADDRESS_2          string,
        ACCT_CITY               string,
        ACCT_STATE              string,
        ACCT_ZIP                string,
        PHY_NAME                string,
        NPI                     string,
        MARKET_TYPE             string,
        SPECIALTY               string,
        DIAGNOSIS_CODE          string,
        ICD_CODESET_IND         string,
        LOINC_CODE              string,
        LOCAL_RESULT_CODE       string,
        RESULT_NAME             string,
        RESULT_VALUE_A          string,
        UNITS                   string,
        REF_RANGE_LOW           string,
        REF_RANGE_HIGH          string,
        REF_RANGE_ALPHA         string,
        ABNORMAL_IND            string,
        HIPAA_COMMENT           string,
        FASTING_INDICATOR       string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = '\t'
    )
    STORED AS TEXTFILE
    LOCATION '{}'
    tblproperties ('skip.header.line.count'='1')
    '''

    schema = kwargs['schema']
    table_name = kwargs['ti'].xcom_pull(task_ids='get_filename', key='filename').split('/')[-1][:-4].lower()
    print schema, table_name
    queries = [
        'DROP TABLE IF EXISTS {}.{}'.format(schema, table_name),
        create_table_statement_template.format(schema, table_name, S3_DATA_STAGED_URL_TEMPLATE)
    ]

    print queries[1]
    hive.hive_execute(queries)


create_table = PythonOperator(
    task_id = 'create_table',
    provide_context = True,
    python_callable = do_create_table,
    op_kwargs = {
        'schema'    : 'dev' if HVDAG.HVDAG.airflow_env =='test' else 'aln441'
    },
    dag = mdag
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

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_data_file_dag.set_upstream(validate_data)
unzip_data_file.set_upstream(fetch_data_file_dag)
get_filename.set_upstream(unzip_data_file)
push_file_dag.set_upstream(get_filename)
create_table.set_upstream(push_file_dag)
clean_up_workspace.set_upstream(push_file_dag)
