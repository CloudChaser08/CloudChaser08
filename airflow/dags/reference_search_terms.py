from airflow.operators import PythonOperator
from datetime import datetime, timedelta

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

DAG_NAME = 'reference_search_terms_mapping'

NUM_NODES = 5
NODE_TYPE = 'm4.4xlarge'
EBS_VOLUME_SIZE = '200'

INSTALL_BOTO3_STEP = (
    'Type=CUSTOM_JAR,Name="Install boto3",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[sudo,pip,install,boto3]')


def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(
        DAG_NAME, NUM_NODES, NODE_TYPE, EBS_VOLUME_SIZE, 'reference_search_terms_update', connected_to_metastore=True)

    emr_utils.run_steps(DAG_NAME, [INSTALL_BOTO3_STEP])


def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(DAG_NAME)


def do_run_pyspark_routine(ds, **kwargs):
    emr_utils.run_script(
        DAG_NAME, kwargs['pyspark_script_name'], [], None
    )


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 1),
    'end_date': None,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 1 1,4,7,10 *',
    default_args=default_args
)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

pull_procedure_icd10 = PythonOperator(
    task_id='pull_procedure_icd10',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    op_kwargs={
        'pyspark_script_name': '/home/hadoop/spark/reference/pull_procedure_icd10.py',
    },
    dag=mdag
)

pull_procedure = PythonOperator(
    task_id='pull_procedure',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    op_kwargs={
        'pyspark_script_name': '/home/hadoop/spark/reference/pull_procedure.py',
    },
    dag=mdag
)

pull_diagnosis = PythonOperator(
    task_id='pull_diagnosis',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    op_kwargs={
        'pyspark_script_name': '/home/hadoop/spark/reference/pull_diagnosis.py',
    },
    dag=mdag
)

pull_loinc = PythonOperator(
    task_id='pull_loinc',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    op_kwargs={
        'pyspark_script_name': '/home/hadoop/spark/reference/pull_loinc.py',
    },
    dag=mdag
)

pull_ndc = PythonOperator(
    task_id='pull_ndc',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    op_kwargs={
        'pyspark_script_name': '/home/hadoop/spark/reference/pull_ndc_ref.py',
    },
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)


pull_procedure_icd10.set_upstream(create_cluster)
pull_procedure.set_upstream(pull_procedure_icd10)
pull_diagnosis.set_upstream(pull_procedure)
pull_loinc.set_upstream(pull_diagnosis)
pull_ndc.set_upstream(pull_loinc)
delete_cluster.set_upstream(pull_ndc)
