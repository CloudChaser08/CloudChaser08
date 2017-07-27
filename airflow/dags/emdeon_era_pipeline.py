from airflow.models import Variable
from airflow.operators import BashOperator, SubDagOperator, PythonOperator
from datetime import datetime, timedelta
import os

import subdags.emdeon_validate_fetch_file
import subdags.split_push_files as split_push_files
import subdags.s3_fetch_file as s3_fetch_file
import subdags.update_analytics_db as update_analytics_db
import util.decompression as decompression
import common.HVDAG as HVDAG

for m in [
        subdags.emdeon_validate_fetch_file, decompression,
        HVDAG, split_push_files, s3_fetch_file, update_analytics_db
]:
    reload(m)

from subdags.emdeon_validate_fetch_file import emdeon_validate_fetch_file

airflow_env = {
    'prod' : 'prod',
    'test' : 'test',
    'dev'  : 'dev'
}[Variable.get("AIRFLOW_ENV", default_var='dev')]

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/webmd/era/{}/'
DAG_NAME='emdeon_era_pipeline'
DATATYPE='era'

if airflow_env == 'test':
    S3_TRANSACTION_RAW_PATH='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/raw/transactions/'
    S3_TRANSACTION_MFT_RAW_PATH='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/raw/transactions/'
    S3_TRANSACTION_SERVICELINE_DESTINATION ='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/out/{}/{}/{}/servicelines/'
    S3_TRANSACTION_CLAIM_DESTINATION ='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/out/{}/{}/{}/claims/'
    S3_LINK_RAW_PATH='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/raw/link/'
    S3_LINK_MFT_RAW_PATH='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/raw/link/'
    S3_LINK_DESTINATION ='s3://salusv/testing/dewey/airflow/e2e/emdeon/era/out/{}/{}/{}/link/'
else:
    S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/era/emdeon/transactions/'
    S3_TRANSACTION_MFT_RAW_PATH='s3://healthverity/incoming/era/emdeon/transactions/'
    S3_TRANSACTION_SERVICELINE_DESTINATION ='s3://salusv/incoming/era/emdeon/{}/{}/{}/servicelines/'
    S3_TRANSACTION_CLAIM_DESTINATION ='s3://salusv/incoming/era/emdeon/{}/{}/{}/claims/'
    S3_LINK_RAW_PATH='s3://healthverity/incoming/era/emdeon/link/'
    S3_LINK_MFT_RAW_PATH='s3://healthverity/incoming/era/emdeon/link/'
    S3_LINK_DESTINATION ='s3://salusv/incoming/era/emdeon/{}/{}/{}/link/'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD ERA transaction file'
TRANSACTION_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_CS_deid.dat.gz'
TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE='{}_AF_ERA_CF_ON_CS_deid.dat'
TRANSACTION_SERVICELINE_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_S_deid.dat'
TRANSACTION_CLAIM_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_C_deid.dat'
TRANSACTION_DAG_NAME='validate_fetch_transaction_file'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Transaction MFT file
TRANSACTION_MFT_FILE_DESCRIPTION='WebMD ERA transaction mft file'
TRANSACTION_MFT_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_CS_deid.dat.mft'
TRANSACTION_MFT_DAG_NAME='validate_fetch_transaction_mft_file'
MINIMUM_TRANSACTION_MFT_FILE_SIZE=15

# Linking file
LINK_FILE_DESCRIPTION='WebMD ERA Linking file'
LINK_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_Link_deid.dat.gz'
LINK_FILE_NAME_UNZIPPED_TEMPLATE='{}_AF_ERA_CF_ON_Link_deid.dat'
LINK_DAG_NAME='validate_fetch_link_file'
MINIMUM_LINK_FILE_SIZE=500

# Linking MFT file
LINK_MFT_FILE_DESCRIPTION='WebMD ERA Linking mft file'
LINK_MFT_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_Link_deid.dat.mft'
LINK_MFT_DAG_NAME='validate_fetch_link_mft_file'
MINIMUM_LINK_MFT_FILE_SIZE=15

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 7, 26, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *",
    default_args=default_args
)

# Validate and fetch all files from the SFTP server
if airflow_env != 'test':
    validate_fetch_transaction_config = {
        'tmp_path_template' : TMP_PATH_TEMPLATE,
        's3_raw_path' : S3_TRANSACTION_RAW_PATH,
        'file_name_template' : TRANSACTION_FILE_NAME_TEMPLATE,
        'datatype' : DATATYPE,
        'minimum_file_size' : MINIMUM_TRANSACTION_FILE_SIZE,
        'file_description' : TRANSACTION_FILE_DESCRIPTION
    }

    validate_fetch_transaction_file_dag = SubDagOperator(
        subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_DAG_NAME, default_args['start_date'],
                                          mdag.schedule_interval, validate_fetch_transaction_config),
        task_id=TRANSACTION_DAG_NAME,
        retries=0,
        dag=mdag
    )

    validate_fetch_transaction_mft_config = {
        'tmp_path_template' : TMP_PATH_TEMPLATE,
        's3_raw_path' : S3_TRANSACTION_MFT_RAW_PATH,
        'file_name_template' : TRANSACTION_MFT_FILE_NAME_TEMPLATE,
        'datatype' : DATATYPE,
        'minimum_file_size' : MINIMUM_TRANSACTION_MFT_FILE_SIZE,
        'file_description' : TRANSACTION_MFT_FILE_DESCRIPTION
    }

    validate_fetch_transaction_mft_file_dag = SubDagOperator(
        subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_MFT_DAG_NAME, default_args['start_date'],
                                          mdag.schedule_interval, validate_fetch_transaction_mft_config),
        task_id=TRANSACTION_MFT_DAG_NAME,
        trigger_rule='all_done',
        retries=0,
        dag=mdag
    )

    validate_fetch_link_config = {
        'tmp_path_template' : TMP_PATH_TEMPLATE,
        's3_raw_path' : S3_LINK_RAW_PATH,
        'file_name_template' : LINK_FILE_NAME_TEMPLATE,
        'datatype' : DATATYPE,
        'minimum_file_size' : MINIMUM_LINK_FILE_SIZE,
        'file_description' : LINK_FILE_DESCRIPTION
    }

    validate_fetch_link_file_dag = SubDagOperator(
        subdag=emdeon_validate_fetch_file(
            DAG_NAME, LINK_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_link_config
        ),
        task_id=LINK_DAG_NAME,
        trigger_rule='all_done',
        retries=0,
        dag=mdag
    )

    validate_fetch_link_mft_config = {
        'tmp_path_template' : TMP_PATH_TEMPLATE,
        's3_raw_path' : S3_LINK_MFT_RAW_PATH,
        'file_name_template' : LINK_MFT_FILE_NAME_TEMPLATE,
        'datatype' : DATATYPE,
        'minimum_file_size' : MINIMUM_LINK_MFT_FILE_SIZE,
        'file_description' : LINK_MFT_FILE_DESCRIPTION
    }

    validate_fetch_link_mft_file_dag = SubDagOperator(
        subdag=emdeon_validate_fetch_file(
            DAG_NAME, LINK_MFT_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_link_mft_config
        ),
        task_id=LINK_MFT_DAG_NAME,
        trigger_rule='all_done',
        retries=0,
        dag=mdag
    )

#
# POST-SFTP TASKS
#

def insert_execution_date_function(file_name_template):
    def out(ds, k):
        return file_name_template.format(k['yesterday_ds_nodash'])
    return out


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['yesterday_ds_nodash'][0:4],
        kwargs['yesterday_ds_nodash'][4:6],
        kwargs['yesterday_ds_nodash'][6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)
    return out


def get_tmp_dir(ds, k):
    return TMP_PATH_TEMPLATE.format(k['ds_nodash'])


# grab files from healthverity/incoming
def generate_fetch_dag(
        task_id, s3_path_template, file_name_template
):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TMP_PATH_TEMPLATE + task_id + '/',
                'expected_file_name_func': insert_execution_date_function(
                    file_name_template
                ),
                's3_prefix'              : s3_path_template,
                's3_bucket'              : 'salusv' if airflow_env == 'test' else 'healthverity'
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_transaction_file = generate_fetch_dag(
    'transaction', '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]), TRANSACTION_FILE_NAME_TEMPLATE
)
fetch_link_file = generate_fetch_dag(
    'link', '/'.join(S3_LINK_RAW_PATH.split('/')[3:]), LINK_FILE_NAME_TEMPLATE
)


# process these files
def do_unzip_file(task_id, file_name_template):
    def out(ds, **kwargs):
        tmp_path = get_tmp_dir(ds, kwargs) + task_id + '/'
        file_path = tmp_path + file_name_template.format(kwargs['yesterday_ds_nodash'])
        decompression.decompress_gzip_file(file_path)
    return PythonOperator(
        task_id='unzip_' + task_id + '_file',
        provide_context=True,
        python_callable=out,
        dag=mdag
    )


unzip_transaction_file = do_unzip_file('transaction', TRANSACTION_FILE_NAME_TEMPLATE)
unzip_link_file = do_unzip_file('link', LINK_FILE_NAME_TEMPLATE)


def generate_parse_transactions_step():
    def out(ds, **kwargs):
        """
        Split transaction file into 2 files - 'claims' and 'servicelines'
        based on the 2nd field value ('C' and 'S', respectively)
        """
        tmp_path = get_tmp_dir(ds, kwargs)
        transaction_tmp_path = tmp_path + 'transaction/'
        serviceline_tmp_path = tmp_path + 'serviceline/'
        claim_tmp_path = tmp_path + 'claim/'

        # these haven't been created yet
        os.mkdir(serviceline_tmp_path)
        os.mkdir(claim_tmp_path)

        transaction_file = transaction_tmp_path + insert_execution_date_function(TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE)(ds, kwargs)
        serviceline_file = serviceline_tmp_path + insert_execution_date_function(TRANSACTION_SERVICELINE_FILE_NAME_TEMPLATE)(ds, kwargs)
        claim_file = claim_tmp_path + insert_execution_date_function(TRANSACTION_CLAIM_FILE_NAME_TEMPLATE)(ds, kwargs)

        with open(serviceline_file, 'w') as serviceline, open(claim_file, 'w') as claim, open(transaction_file, 'r') as transactions:
            for line in transactions:
                if '|' in line and line.split('|')[1] == 'S':
                    serviceline.write(line)
                elif '|' in line and line.split('|')[1] == 'C':
                    claim.write(line)
    return PythonOperator(
        task_id='parse_transaction_file',
        provide_context=True,
        python_callable=out,
        dag=mdag
    )


parse_transactions = generate_parse_transactions_step()


def generate_split_dag(task_id, file_name_unzipped_template, s3_destination):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : lambda ds, k: get_tmp_dir(ds, k) + task_id + '/',
                'file_paths_to_split_func' : lambda ds, k: [
                    get_tmp_dir(ds, k) + task_id + '/' +
                    insert_execution_date_function(file_name_unzipped_template)(ds, k)
                ],
                's3_prefix_func'           : insert_current_date_function(
                    s3_destination
                ),
                'num_splits'               : 20
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_push_claims = generate_split_dag(
    'claim', TRANSACTION_CLAIM_FILE_NAME_TEMPLATE, S3_TRANSACTION_CLAIM_DESTINATION
)
split_push_servicelines = generate_split_dag(
    'serviceline', TRANSACTION_SERVICELINE_FILE_NAME_TEMPLATE, S3_TRANSACTION_SERVICELINE_DESTINATION
)
split_push_link = generate_split_dag(
    'link', LINK_FILE_NAME_UNZIPPED_TEMPLATE, S3_LINK_DESTINATION
)


clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ds_nodash }}')),
    trigger_rule='all_done',
    dag=mdag
)


def generate_update_analytics_db_dag(table_name, era_table_type):
    sql_new_template = """
    ALTER TABLE {table_name} ADD PARTITION (part_processdate='{year}/{month}/{day}')
    LOCATION 's3a://salusv/incoming/era/emdeon/{year}/{month}/{day}/{era_table_type}/'
    """

    def sql_command_func(ds, k):
        current_date = insert_current_date('{}{}{}', k)
        return sql_new_template.format(
            table_name=table_name,
            era_table_type=era_table_type,
            year=current_date[0:4],
            month=current_date[4:6],
            day=current_date[6:8]
        )

    return SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db_' + era_table_type,
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : sql_command_func
            }
        ),
        task_id='update_analytics_db_' + era_table_type,
        dag=mdag
    )


if airflow_env != 'test':
    update_analytics_db_claims = generate_update_analytics_db_dag('era_emdeon_claims', 'claims')
    update_analytics_db_serviceline = generate_update_analytics_db_dag('era_emdeon_service', 'servicelines')
    update_analytics_db_link = generate_update_analytics_db_dag('era_emdeon_link', 'link')


if airflow_env != 'test':
    validate_fetch_transaction_mft_file_dag.set_downstream(clean_up_workspace)
    validate_fetch_link_mft_file_dag.set_downstream(clean_up_workspace)

    validate_fetch_transaction_file_dag.set_downstream(fetch_transaction_file)
    validate_fetch_link_file_dag.set_downstream(fetch_link_file)

    clean_up_workspace.set_downstream([update_analytics_db_claims, update_analytics_db_serviceline, update_analytics_db_link])

fetch_transaction_file.set_downstream(unzip_transaction_file)
fetch_link_file.set_downstream(unzip_link_file)


parse_transactions.set_upstream(unzip_transaction_file)

split_push_claims.set_upstream(parse_transactions)
split_push_servicelines.set_upstream(parse_transactions)
split_push_link.set_upstream(unzip_link_file)

clean_up_workspace.set_upstream([split_push_claims, split_push_link, split_push_servicelines])
