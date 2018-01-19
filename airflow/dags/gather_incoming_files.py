from airflow.operators import PythonOperator
from datetime import datetime
import re
import logging
import json

# hv-specific modules
import common.HVDAG as HVDAG
import dags.util.s3_utils as s3_utils

for m in [HVDAG, s3_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'gather_incoming_files'

CONFIG_FILE = 'resources/incoming_files_conf.json'

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

class IncomingFileConfig:
    def __init__(self, feed_name, src_path, dest_path, file_name_regexes):
        self.feed_name = feed_name
        self.src_path = src_path
        self.dest_path = dest_path
        self.file_name_regexes = file_name_regexes.replace('//', '/')

    def _is_relevant_file(self, filename):
        for regex in self.file_name_regexes:
            if re.match(regex, filename):
                return True
        return False

    def get_new_files(self):
        existing_files = s3_utils.list_s3_bucket_files(self.dest_path)
        return [
            f for f in s3_utils.list_s3_bucket_files(self.src_path)
            if f not in existing_files and self._is_relevant_file(f)
        ]


def generate_detect_move_task(config):
    def execute(ds, **kwargs):
        new_files = config.get_new_files()
        if new_files:
            logging.info("Moving {} files for {}".format(str(len(new_files)), config.feed_name))
            for f in new_files:
                s3_utils.copy_file(config.src_path + f, config.dest_path)
        else:
            logging.info("No new files found for {}".format(config.feed_name))

    return PythonOperator(
        task_id='copy_' + config.feed_name + '_files',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )

with open(CONFIG_FILE, 'r') as incoming_files_config_file:
    tasks = [
        generate_detect_move_task(IncomingFileConfig(**conf))
        for conf in json.load(incoming_files_config_file)
    ]
