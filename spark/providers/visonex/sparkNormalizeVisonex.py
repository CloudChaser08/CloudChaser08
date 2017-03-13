#! /usr/bin/python
import os
import argparse
import time
from datetime import timedelta, datetime, date
from spark.runner import Runner
from spark.spark import init
import spark.helpers.create_date_validation_table \
    as date_validator


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            '/home/hadoop/spark/providers/visonex/',
            relative_filename
        )
    )


# init
spark, sqlContext = init("Visonex")

# initialize runner
runner = Runner(sqlContext)

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--date', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--debug', default=False, action='store_true')
args = parser.parse_args()

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

input_path = 's3://salusv/incoming/emr/visonex/{}/{}/{}/'.format(
    str(date_obj.year),
    str(date_obj.month).zfill(2),
    str(date_obj.day).zfill(2)
)

runner.run_spark_script(
    get_rel_path('load_transactions.sql'), [
        ['input_path', input_path]
    ]
)

spark.sparkContext.stop()
