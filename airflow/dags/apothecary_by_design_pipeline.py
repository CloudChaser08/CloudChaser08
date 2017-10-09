from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta

#hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.detect_move_normalize as detect_move_normalize
import subdags.queue_up_for_matching as queue_up_for_matching

for m in [HVDAG, s3_validate_file, s3_fetch_file, detect_move_normalize,
          queue_up_for_matching]:
    reload(m)

DAG_NAME = 'apothecary_by_design_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 10, 8),    #TODO: determine when we start
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = None,               #TODO: determine when we start
    default_args = default_args
)

if HVDAG.HVDAG.aiflow_env == 'test':
    test_loc  = 's3://salusv/testing/dewey/airflow/e2e/apothecary_by_design/pharmacyclaims/'
    S3_TRANSACTION_RAW_URL = test_loc + 'raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = test_loc + 'out/{}/{}/{}/'
    S3_PAYLOAD_DEST = test_loc + 'payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/apothecary_by_design/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/apothecarybydesign/'

TMP_PATH_TEMPLATE = '/tmp/apothecary_by_design/pharmacyclaims/{}/'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'

TRANSACTION_FILE_NAME_TEMPLATE = 'hv_export_data.txt'
DEID_FILE_NAME_TEMPLATE = 'hv_export_po_deid.txt'

def get_formatted_date(ds, kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs)

    return out


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['ds_nodash'][0:4],
        kwargs['ds_nodash'][4:6],
        kwargs['ds_nodash'][6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)

    return out


get_tmp_dir = insert_formatted_date_function(TRANSACTION_TMP_PATH_TEMPLATE)
