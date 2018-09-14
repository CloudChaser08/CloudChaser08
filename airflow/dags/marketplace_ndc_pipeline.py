import common.HVDAG as HVDAG

from datetime import datetime, timedelta
import logging

from airflow.operators import SubDagOperator

import subdags.run_pyspark_routine as run_pyspark_routine
import subdags.update_analytics_db as update_analytics_db
import util.date_utils as date_utils

for m in [
    run_pyspark_routine, update_analytics_db,
    date_utils, HVDAG
    ]:
    reload(m)

DAG_NAME = 'marketplace_ndc'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 9, 14, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 14 * * *',
    default_args=default_args
)

S3_OUTPUT_LOCATION_TEMPLATE = 's3://salusv/warehouse/parquet/marketplace/ndc/{}{}{}/'
MARKETPLACE_NDC_DAY_OFFSET = 1

def norm_args(ds, kwargs):
    base = ['--output_loc', date_utils.insert_date_into_template(S3_OUTPUT_LOCATION_TEMPLATE, kwargs, day_offset=MARKETPLACE_NDC_DAY_OFFSET)]
    return base


create_output = SubDagOperator(
    subdag=run_pyspark_routine.run_pyspark_routine(
        DAG_NAME,
        'create_output',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'EMR_CLUSTER_NAME_FUNC' : date_utils.generate_insert_date_into_template_function(
                'marketplace_ndc_{}{}{}',
                day_offset=MARKETPLACE_NDC_DAY_OFFSET
            ),
            'PYSPARK_SCRIPT_NAME'   : '/home/hadoop/spark/reference/marketplace_ndc/sparkCreateMarketplaceNDC.py',
            'PYSPARK_ARGS_FUNC'     : norm_args,
            'NUM_NODES'             : '2',
            'NODE_TYPE'             : 'm4.xlarge',
            'EBS_VOLUME_SIZE'       : '50',
            'PURPOSE'               : 'reference_data_update',
            'CONNECT_TO_METASTORE'  : True
        }
    ),
    task_id='create_output',
    dag=mdag
)

sql_template = """
    ALTER TABLE marketplace_ndc SET LOCATION {}
""".format(S3_OUTPUT_LOCATION_TEMPLATE)
update_analytics_db = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' : date_utils.generate_insert_date_into_template_function(
                sql_template,
                day_offset=MARKETPLACE_NDC_DAY_OFFSET
            )
        }
    ),
    task_id='update_analytics_db',
    dag=mdag
)

### DAG STRUCTURE ###
update_analytics_db.set_upstream(create_output)
