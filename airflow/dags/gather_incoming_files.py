from airflow.operators import PythonOperator
from airflow.models import Variable
from datetime import datetime
import re
import logging
import json
import os

# hv-specific modules
import common.HVDAG as HVDAG
import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils

for m in [HVDAG, s3_utils, sftp_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'gather_incoming_files'

TMP_DIR = '/tmp/incoming-files/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 20),
    'retries': 0,
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0,30 * * * *",
    default_args=default_args
)

CONFIG_FILE = os.getenv("AIRFLOW_HOME") + '/dags/test-e2e/resources/incoming_files_conf.json' \
    if HVDAG.HVDAG.airflow_env == 'test' \
    else os.getenv("AIRFLOW_HOME") + '/dags/resources/incoming_files_conf.json'


class SFTPConnectionConfig:
    def __init__(self, path, host, user, password):
        self.path = path
        self.host = host
        self.user = user
        self.password = password

    def asdict(self):
        return {
            'path': self.path, 'host': self.host, 'user': self.user, 'password': self.password
        }


class S3ConnectionConfig:
    def __init__(self, path):
        self.path = path

    def asdict(self):
        return {'path': self.path}


class IncomingFileConfig:
    def __init__(
            self, name, src_system, src_conf,
            dest_path, file_name_regexes, **kwargs
    ):
        self.name = name
        self.src_system = src_system

        if self.src_system == 'sftp':
            self.src_conf = SFTPConnectionConfig(**json.loads(
                Variable.get(kwargs['src_conf']['airflow_conf_variable'])
            ))
        elif self.src_system == 's3':
            self.src_conf = S3ConnectionConfig(**src_conf)

        self.dest_path = dest_path
        self.file_name_regexes = [regex.replace('//', '/') for regex in file_name_regexes]

    def _is_relevant_file(self, filename):
        for regex in self.file_name_regexes:
            if re.match(regex, filename):
                return True
        return False

    def get_fetch_func(self):
        def out(src_filename, dest_filepath):
            if self.src_system == 's3':
                s3_utils.copy_file(self.src_conf.path + src_filename, dest_filepath)
            elif self.src_system == 'sftp':
                fetch_conf = self.src_conf.asdict()
                fetch_conf.update({
                    'abs_internal_filepath': TMP_DIR,
                    'abs_external_filepath': self.src_conf.path + src_filename
                })
                sftp_utils.fetch_file(**fetch_conf)
                s3_utils.copy_file(TMP_DIR + src_filename, dest_filepath)
                os.rm(TMP_DIR + src_filename)
        return out

    def get_new_files(self):
        existing_files = s3_utils.list_s3_bucket_files(self.dest_path)
        if self.src_system == 's3':
            src_file_list = s3_utils.list_s3_bucket_files(self.src_conf.path)
        elif self.src_system == 'sftp':
            src_file_list = sftp_utils.list_path(**self.src_conf.asdict())
        return [
            f for f in src_file_list if f not in existing_files and self._is_relevant_file(f)
        ]


def generate_detect_move_task(config):
    def execute(ds, **kwargs):
        new_files = config.get_new_files()
        if new_files:
            logging.info("Moving {} files for {}".format(str(len(new_files)), config.name))
            for f in new_files:
                config.get_fetch_func()(f, config.dest_path)
        else:
            logging.info("No new files found for {}".format(config.name))

    return PythonOperator(
        task_id='copy_' + config.name + '_files',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


with open(CONFIG_FILE, 'r') as incoming_files_config_file:
    tasks = [
        generate_detect_move_task(IncomingFileConfig(**conf))
        for conf in json.load(incoming_files_config_file)
    ]
