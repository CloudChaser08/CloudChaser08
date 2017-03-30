#! /usr/bin/python
import subprocess
import argparse
import time
import spark.helpers.file_utils as file_utils
import spark.spark
from spark.runner import Runner

# init
spark, sqlContext = spark.init("Caris")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

runner.run_spark_script(get_rel_path(
    '../../common/lab_common_model.sql'
))

subprocess.call(' '.join(psql + ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    ['-v', 'input_path="\'' + args.input_path + '\'"'] +
    ['-v', 'matching_path="\'' + args.matching_path + '\'"'] +
    [db, '<', 'load_transactions.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM lab_common_model\'"']
    + [db, '<', '../redshift_norm_common/unload_common_model.sql']
), shell=True)
