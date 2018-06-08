from airflow.models import Variable
from airflow.operators import PythonOperator, ExternalTaskSensor, SubDagOperator
from datetime import datetime, timedelta
import json

import util.emr_utils as emr_utils
import util.date_utils as date_utils
import util.sftp_utils as sftp_utils
import util.slack as slack
import subdags.s3_fetch_file as s3_fetch_file
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import common.HVDAG as HVDAG
for m in [
        emr_utils, HVDAG, date_utils, sftp_utils, s3_fetch_file,
        clean_up_tmp_dir, slack
]:
    reload(m)

DAG_NAME='celgene_delivery'

EMR_CLUSTER_NAME_TEMPLATE='delivery_cluster-celgene242-{}{}{}'
NUM_NODES=5
NODE_TYPE='m4.2xlarge'
EBS_VOLUME_SIZE='100'

CELGENE_DAY_OFFSET = 7

TMP_PATH_TEMPLATE = '/tmp/projects/celgene-hv000242/{}{}{}/'
PHARMACY_STAGING_TEMPLATE = TMP_PATH_TEMPLATE + 'pharmacy_claims/'
NPPES_STAGING_TEMPLATE = TMP_PATH_TEMPLATE + 'nppes/'

S3_PATH_TEMPLATE = 's3://salusv/projects/celgene/hv000242/delivery/{}{}{}/'
PHARMACY_S3_PATH_TEMPLATE = S3_PATH_TEMPLATE + 'pharmacy_claims/'
NPPES_S3_PATH_TEMPLATE = S3_PATH_TEMPLATE + 'nppes/'

PHARMACY_FILENAME = 'pharmacy_claims_{}{}{}.csv.gz'
NPPES_FILENAME = 'nppes_{}{}{}.csv.gz'


def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(
        date_utils.insert_date_into_template(EMR_CLUSTER_NAME_TEMPLATE, kwargs, day_offset=CELGENE_DAY_OFFSET),
        NUM_NODES, NODE_TYPE, EBS_VOLUME_SIZE, 'delivery', connected_to_metastore=True)


def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(
        date_utils.insert_date_into_template(EMR_CLUSTER_NAME_TEMPLATE, kwargs, day_offset=CELGENE_DAY_OFFSET)
    )


def do_run_pyspark_export_routine(ds, **kwargs):
    emr_utils.export(
        date_utils.insert_date_into_template(
            EMR_CLUSTER_NAME_TEMPLATE, kwargs, day_offset=CELGENE_DAY_OFFSET
        ), kwargs['pyspark_script_name'], kwargs['pyspark_args_func'](ds, kwargs)
    )


def get_export_args(ds, kwargs):
    return ['--date', date_utils.insert_date_into_template(
        '{}-{}-{}', kwargs, day_offset=CELGENE_DAY_OFFSET
    )]


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 12, 19, 19),
    'end_date': datetime(2018, 8, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 19 * * 2', # tuesday afternoons
    default_args=default_args
)

apothecary_by_design = ExternalTaskSensor(
    task_id='wait_for_abd',
    external_dag_id='apothecary_by_design_pipeline',
    external_task_id='update_analytics_db',
    execution_delta=timedelta(days=1, hours=3), # abd runs on mondays
    dag=mdag
)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

run_pyspark_export_routine = PythonOperator(
    task_id='run_pyspark_export_routine',
    provide_context=True,
    python_callable=do_run_pyspark_export_routine,
    op_kwargs={
        'pyspark_script_name' : '/home/hadoop/spark/delivery/celgene_hv000242/sparkExtractCelgene.py',
        'pyspark_args_func'   : get_export_args
    },
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)


def generate_fetch_dag(task_id, temp_path_template, s3_path_template, s3_file_name_template):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : temp_path_template,
                'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                    s3_file_name_template, day_offset = CELGENE_DAY_OFFSET
                ),
                's3_prefix_func'         : date_utils.generate_insert_date_into_template_function(
                    '/'.join(s3_path_template.split('/')[3:]), day_offset = CELGENE_DAY_OFFSET
                ),
                's3_bucket'              : s3_path_template.split('/')[2]
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_pharmacyclaims_output = generate_fetch_dag(
    'pharmacyclaims', PHARMACY_STAGING_TEMPLATE, PHARMACY_S3_PATH_TEMPLATE, PHARMACY_FILENAME
)
fetch_nppes_output = generate_fetch_dag(
    'nnpes', NPPES_STAGING_TEMPLATE, NPPES_S3_PATH_TEMPLATE, NPPES_FILENAME
)


def generate_sftp_upload_task(task_id, temp_path_template, file_name_template):
    def do_sftp_upload(ds, **kwargs):
        sftp_config = json.loads(Variable.get('celgene sftp configuration'))

        sftp_utils.upload_file(
            date_utils.insert_date_into_template(temp_path_template, kwargs)
            + date_utils.insert_date_into_template(file_name_template, kwargs, day_offset=CELGENE_DAY_OFFSET),
            sftp_config['path'], host=sftp_config['host'], username=sftp_config['username'],
            password=sftp_config['password']
        )

    return PythonOperator(
        task_id='sftp_upload_' + task_id,
        provide_context=True,
        python_callable=do_sftp_upload,
        dag=mdag
    )


sftp_upload_pharmacyclaims = generate_sftp_upload_task(
    'pharmacyclaims', PHARMACY_STAGING_TEMPLATE, PHARMACY_FILENAME
)
sftp_upload_nppes = generate_sftp_upload_task(
    'nppes', NPPES_STAGING_TEMPLATE, NPPES_FILENAME
)


def generate_delivery_alert_task():
    def do_send_message(ds, **kwargs):
        slack.send_message(
            'delivery', date_utils.insert_date_into_template(
                '@reynaklesh Celgene {}{}{} has been delivered', kwargs,
                day_offset=CELGENE_DAY_OFFSET
            )
        )

    return PythonOperator(
        task_id='slack_alert_delivery_complete',
        provide_context=True,
        python_callable=do_send_message,
        dag=mdag
    )


slack_alert_delivery_complete = generate_delivery_alert_task()

clean_up = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        DAG_NAME,
        'clean_up',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE
        }
    ),
    task_id='clean_up',
    dag=mdag
)

create_cluster.set_upstream(apothecary_by_design)
run_pyspark_export_routine.set_upstream(create_cluster)
delete_cluster.set_upstream(run_pyspark_export_routine)
run_pyspark_export_routine.set_downstream([
    fetch_pharmacyclaims_output, fetch_nppes_output
])
sftp_upload_pharmacyclaims.set_upstream(fetch_pharmacyclaims_output)
sftp_upload_nppes.set_upstream(fetch_nppes_output)
slack_alert_delivery_complete.set_upstream([sftp_upload_nppes, sftp_upload_pharmacyclaims])
clean_up.set_upstream([sftp_upload_nppes, sftp_upload_pharmacyclaims])
