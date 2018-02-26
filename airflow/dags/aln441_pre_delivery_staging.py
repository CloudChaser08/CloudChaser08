from airflow.models import Variable

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.s3_push_files as s3_push_files
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import util.decompression as decompression
import util.s3_utils as s3_utils
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, s3_push_files
          HVDAG, decompression, s3_utils,
          clean_up_tmp_dir, date_utils]:
    reload(m)

DAG_NAME = 'aln441_pre_delivery_staging'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 26, 17),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 15 * * *",
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

ALN441_PRE_DELIVERY_DAY_OFFSET = 7

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

def get_files_matching_template(template, ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    file_regex = date_utils.insert_date_into_template(template, kwargs, day_offset = ALN441_PRE_DELIVERY_DAY_OFFSET)
    return [file_dir + f for f in os.listdir(file_dir) if re.search(file_regex, f)]


def generate_file_validation_task(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func'   :
                    date_utils.generate_insert_date_into_template_function(
                        path_template,
                        day_offset = ALN441_PRE_DELIVERY_DAY_OFFSET
                ),
                'file_name_pattern_func'    : lambda ds, k: (
                    DATA_FILE_NAME_REGEX
                ),
                'minimum_file_size'         : minimum_file_size,
                's3_prefix'                 : '/'.join(S3_DATA_RAW_URL.split('/')[3:]),
                's3_bucket'                 : S3_DATA_RAW_URL.split('/')[2],
                'file_description'          : 'aln441 pre delivery ' + task_id + ' file',
                'regex_name_match'          : True,
                'quiet_retries'             : 24
            }
        ),
        task_id='validate_' + task_id + '_file',
        retries=6,
        retry_delay=timedelta(minutes=2),
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_data = generate_file_validation_task(
            'data', DATA_FILE_NAME_TEMPLATE,
        250
    )

fetch_deid_file_dag = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_deid_file',
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
    task_id='fetch_deid_file',
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
