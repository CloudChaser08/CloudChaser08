from airflow.operators import PythonOperator
from subprocess import check_call
import util.s3_utils as s3_utils
import os
import logging

import common.HVDAG as HVDAG
import util.datadog_utils as datadog

for m in [s3_utils, datadog, HVDAG]:
    reload(m)


def get_parts_dir(ds, k):
    return k['parts_dir_func'](ds, k) if k.get('parts_dir_func') else 'parts'


def do_log_file_volume(dag_name, file_name_pattern_func=None, file_paths_to_split_func=None):
    """Log file volume to datadog.

    Optional `file_pattern_func` and `file_paths_to_split_func` args
    can be used by DAGs that are calling this function outside of the
    split_push_files subdag
    """
    def out(ds, **kwargs):
        if kwargs.get('file_name_pattern_func') or (file_name_pattern_func and file_paths_to_split_func):
            dd = datadog.Datadog()
            file_pattern = (
                file_name_pattern_func if file_name_pattern_func
                else kwargs['file_name_pattern_func']
            )(ds, kwargs)
            file_paths_to_split = (
                file_paths_to_split_func if file_paths_to_split_func
                else kwargs['file_paths_to_split_func']
            )(ds, kwargs)
            for i, filepath in enumerate(file_paths_to_split):
                with open(filepath) as f:
                    row_count = sum(1 for line in f)
                    dd.create_metric(
                        name='airflow.dag.file_row_count',
                        value=row_count,
                        tags=['dag:' + dag_name, 'file:' + file_pattern]
                    )
    return out

def do_create_parts_dir(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs) + get_parts_dir(ds, kwargs) + '/'
    check_call(['mkdir', '-p', tmp_dir])


def do_convert_file_encoding(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    file_paths_to_split = kwargs['file_paths_to_split_func'](ds, kwargs)
    source_encoding = kwargs['source_encoding']
    for fp in file_paths_to_split:
        out = open(fp + '.utf8', 'w')
        check_call([
            'iconv', '-f', source_encoding, '-t', 'UTF-8', fp
        ], stdout=out)
        out.close()
        os.remove(fp)
        os.rename(fp + '.utf8', fp)


def do_split_files(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    file_paths_to_split = kwargs['file_paths_to_split_func'](ds, kwargs)

    logging.info(file_paths_to_split)
    for fp in file_paths_to_split:
        f = fp.split('/')[-1]
        check_call([
            'split', '-n', 'l/' + str(kwargs['num_splits']), fp,
            tmp_dir + get_parts_dir(ds, kwargs) + '/' + f + '.'
        ])

def do_bzip_part_files(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    files = os.listdir(tmp_dir + get_parts_dir(ds, kwargs) + '/')
    for f in files:
        check_call(['lbzip2', '-z', tmp_dir + get_parts_dir(ds, kwargs) + '/' + f])

def do_push_part_files(ds, **kwargs):
    tmp_dir   = kwargs['tmp_dir_func'](ds, kwargs) + get_parts_dir(ds, kwargs) + '/'
    files = os.listdir(tmp_dir)
    for i in xrange(0, len(files), 5):
        processes = []
        for j in xrange(i, min(i + 5, len(files))):
            # This will get passed back to the s3_prefix_func so that unique
            # prefixes can be generated per file
            kwargs['file_to_push'] = files[j]
            s3_prefix = kwargs['s3_prefix_func'](ds, kwargs)
            processes.append(s3_utils.copy_file_async(tmp_dir + files[j], s3_prefix))
        for p in processes:
            p.wait()

def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    file_paths_to_split = kwargs['file_paths_to_split_func'](ds, kwargs)

    for fp in file_paths_to_split:
        check_call(['rm', fp])
    check_call(['rm', '-r', tmp_dir + get_parts_dir(ds, kwargs)])

def split_push_files(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = HVDAG.HVDAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval='@daily',
        start_date=start_date,
        default_args=default_args
    )

    log_file_volume = PythonOperator(
        task_id='log_file_volume',
        provide_context=True,
        python_callable=do_log_file_volume(parent_dag_name),
        op_kwargs=dag_config,
        dag=dag
    )

    create_parts_dir = PythonOperator(
        task_id='create_parts_dir',
        provide_context=True,
        python_callable=do_create_parts_dir,
        op_kwargs=dag_config,
        dag=dag
    )

    convert_file_encoding = PythonOperator(
        task_id='convert_file_encoding',
        provide_context=True,
        python_callable=do_convert_file_encoding,
        op_kwargs=dag_config,
        dag=dag
    )

    split_files = PythonOperator(
        task_id='split_files',
        provide_context=True,
        python_callable=do_split_files,
        op_kwargs=dag_config,
        dag=dag
    )

    bzip_part_files = PythonOperator(
        task_id='bzip_part_files',
        provide_context=True,
        python_callable=do_bzip_part_files,
        op_kwargs=dag_config,
        dag=dag
    )

    push_part_files = PythonOperator(
        task_id='push_part_files',
        provide_context=True,
        python_callable=do_push_part_files,
        op_kwargs=dag_config,
        dag=dag
    )

    clean_up = PythonOperator(
        task_id='clean_up',
        provide_context=True,
        python_callable=do_clean_up,
        op_kwargs=dag_config,
        dag=dag
    )

    create_parts_dir.set_upstream(log_file_volume)
    if 'source_encoding' in dag_config:
        convert_file_encoding.set_upstream(create_parts_dir)
        split_files.set_upstream(convert_file_encoding)
    else:
        split_files.set_upstream(create_parts_dir)
    bzip_part_files.set_upstream(split_files)
    push_part_files.set_upstream(bzip_part_files)
    clean_up.set_upstream(push_part_files)

    return dag
