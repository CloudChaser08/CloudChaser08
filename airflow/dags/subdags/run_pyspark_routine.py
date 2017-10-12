from airflow.operators import PythonOperator

import util.emr_utils as emr_utils
import common.HVDAG as HVDAG

for m in [emr_utils, HVDAG]:
    reload(m)


def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(
        kwargs['EMR_CLUSTER_NAME_FUNC'](ds, kwargs),
        kwargs.get('NUM_NODES', 5),
        kwargs.get('NODE_TYPE', 'm4.xlarge'),
        kwargs.get('EBS_VOLUME_SIZE', 50),
        kwargs.get('PURPOSE', 'none'),
        kwargs.get('CONNECT_TO_METASTORE', False)
    )


def do_run_pyspark_routine(ds, **kwargs):
    emr_utils.run_script(
        kwargs['EMR_CLUSTER_NAME_FUNC'](ds, kwargs),
        kwargs['PYSPARK_SCRIPT_NAME'],
        kwargs['PYSPARK_ARGS_FUNC'](ds, kwargs),
        kwargs.get('SPARK_CONF_ARGS', None)
    )


def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(
        kwargs['EMR_CLUSTER_NAME_FUNC'](ds, kwargs)
    )


def run_pyspark_routine(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    '''
    A DAG that will spin up an EMR cluster, run a pyspark routine on it, and then close the cluster
    Input:
        - parent_dag_name: the name of the dag using this subdag
        - child_dag_name: the name of the subdag
        - start_date: same as parent dag start date
        - schedule interval: same as the parent schedule interval
        - dag_config: A dictionary expecting the following keys (* -> required, o -> optional):
            * EMR_CLUSTER_NAME_FUNC
                - Name of the EMR cluster
            * PYSPARK_SCRIPT_NAME
                - Name of the pyspark script to run
            * PYSPARK_ARGS_FUNC
                - Function that returns the arguments for the script
                  based on input ds & kwargs
            o NUM_NODES (default: 5)
                - Number of nodes for the EMR cluster
            o NODE_TYPE (default: 'm4.xlarge')
                - Type of node for the EMR cluster
            o EBS_VOLUME_SIZE (default: 50)
                - Size of the EBS Storage 
            o PURPOSE (default: 'none')
                - Purpose of the cluster and routine being ran on it
            o CONNECT_TO_METASTORE (default: False)
                - Boolean indicating if we should connect to metastore or not
            o SPARK_CONF_ARGS (default: None)
                - List of configurations for running a pyspark job
    Output:
        - dag: The subdag created here
    '''

    if dag_config is None:
        raise Exception('Must pass in a dictionary dag_config')
    
    if 'EMR_CLUSTER_NAME_FUNC' not in dag_config:
        raise Exception('EMR_CLUSTER_NAME_FUNC must be specified in dag_config')

    if 'PYSPARK_SCRIPT_NAME' not in dag_config:
        raise Exception('PYSPARK_SCRIPT_NAME must be specified in dag_config')

    if 'PYSPARK_ARGS_FUNC' not in dag_config:
        raise Exception('PYSPARK_ARGS_FUNC must be specified in dag_config')

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = HVDAG.HVDAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval = '@daily',
        start_date = start_date,
        default_args = default_args
    )

    create_cluster = PythonOperator(
        task_id = 'create_cluster',
        provide_context = True,
        op_kwargs = dag_config,
        python_callable = do_create_cluster,
        dag = dag
    )

    run_pyspark_routine = PythonOperator(
        task_id = 'run_pyspark_routine',
        provide_context = True,
        op_kwargs = dag_config,
        python_callable = do_run_pyspark_routine,
        dag = dag
    )

    delete_cluster = PythonOperator(
        task_id = 'delete_cluster',
        provide_context = True,
        op_kwargs = dag_config,
        python_callable = do_delete_cluster,
        dag = dag
    )

    ### DAG Structure ###
    run_pyspark_routine.set_upstream(create_cluster)
    delete_cluster.set_upstream(run_pyspark_routine)

    return dag
