from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import re
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.detect_move_normalize as detect_move_normalize
import util.s3_utils as s3_utils
import util.date_utils as date_utils
for m in [s3_validate_file, detect_move_normalize, HVDAG, s3_utils, date_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'nextgen_pipeline'
TMP_PATH_TEMPLATE = '/tmp/nextgen/emr/{}/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 9, 20, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 16 20 * *',
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/ng-lssa/{0}{1}/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/nextgen/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/nextgen/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Nextgen EMR transaction file'
TRANSACTION_FILE_NAME_REGEX = 'NG_LSSA_\d{5}_\d{4}\d{2}\d{2}.txt.gz'

NEXTGEN_FIXED_DAY = 1
NEXTGEN_MONTH_OFFSET = 1

# There are going to be hundreds of files, check that at
# least one is like this
def generate_file_validation_task(
    task_id, minimum_file_size, path_template
):
    return SubDagOperator(
        subdag = s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : lambda ds, k: TRANSACTION_FILE_NAME_REGEX,
                'file_name_pattern_func'  : lambda ds, k: TRANSACTION_FILE_NAME_REGEX,
                'minimum_file_size'       : minimum_file_size,
                's3_prefix_func'          : date_utils.generate_insert_date_into_template_function(
                    '/'.join(path_template.split('/')[3:]), month_offset=NEXTGEN_MONTH_OFFSET
                ),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Nextgen EMR ' + task_id + ' file',
                'regex_name_match'        : True
            }
        ),
        task_id = 'validate_' + task_id + '_file',
        dag = mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction_deltas    = generate_file_validation_task(
        'deltas', 30, S3_TRANSACTION_RAW_URL + 'deltas/'
    )
    validate_transaction_histories = generate_file_validation_task(
        'histories', 30, S3_TRANSACTION_RAW_URL + 'histories/'
    )

def do_copy_to_internal_s3(ds, **kwargs):
    path = date_utils.insert_date_into_template(
        kwargs['path_template'], kwargs, month_offset=NEXTGEN_MONTH_OFFSET
    )
    all_files = s3_utils.list_s3_bucket_files(path)
    regex_pattern = TRANSACTION_FILE_NAME_REGEX
    new_files = [f for f in all_files if re.search(regex_pattern, f)]
    internal_s3_dest = date_utils.insert_date_into_template(
            S3_TRANSACTION_PROCESSED_URL_TEMPLATE, kwargs, fixed_day=1,
            day_offset=-1)

    # Nextgen sends us 1 file per "enterprise", resulting in hundreds of files
    # every time they send us an update. This code will copy the files in
    # in batches to make things go faster (instead of 1 at a time)
    CHUNK_SIZE=10
    i = 0
    copy_ops = []
    while i < len(new_files):
        for j in xrange(i, min(i+CHUNK_SIZE, len(new_files))):
            copy_ops.append(s3_utils.copy_file_async(
                path + new_files[i],
                internal_s3_dest + new_files[i]
            ))
        i = min(i+CHUNK_SIZE, len(new_files))
        for o in copy_ops:
            o.wait()

def generate_file_copying_task(
    task_id, path_template
):
    return PythonOperator(
        task_id = 'copy_' + task_id + '_to_internal_s3',
        python_callable = do_copy_to_internal_s3,
        provide_context = True,
        op_kwargs = {'path_template' : path_template},
        dag = mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    copy_deltas_to_internal_s3 = generate_file_copying_task(
        'deltas', S3_TRANSACTION_RAW_URL + 'deltas/'
    )
    copy_histories_to_internal_s3 = generate_file_copying_task(
        'histories', S3_TRANSACTION_RAW_URL + 'histories/'
    )

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k,
        fixed_day=1, day_offset=-1
    )]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : lambda ds, k: [],
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'c7cd671d-b60e-4cbd-b703-27267bcd9062',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/nextgen/emr/sparkNormalizeNextgenEMR.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True,
            'emr_num_nodes'                     : 5,
            'emr_node_type'                     : 'm4.16xlarge',
            'emr_ebs_volume_size'               : 500,
            'spark_conf_args'                   : ['--conf', 'spark.sql.shuffle.partitions=5000',
                '--conf', 'spark.executor.cores=4', '--conf', 'spark.executor.memory=13G',
                '--conf', 'spark.hadoop.fs.s3a.maximum.connections=1000',
                '--conf', 'spark.files.useFetchCache=false'
            ]
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

### Dag Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    copy_histories_to_internal_s3.set_upstream(validate_transaction_histories)
    copy_deltas_to_internal_s3.set_upstream(validate_transaction_deltas)
    detect_move_normalize_dag.set_upstream([copy_histories_to_internal_s3, copy_deltas_to_internal_s3])
