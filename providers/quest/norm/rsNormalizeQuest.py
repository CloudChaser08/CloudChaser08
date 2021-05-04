#! /usr/bin/python
import subprocess
import argparse
import time
import sys
import os
import hashlib
from datetime import datetime, timedelta


def get_rel_path(relative_filename):
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            relative_filename
        )
    )

sys.path.append(get_rel_path('../../redshift_norm_common'))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--period', type=str)
parser.add_argument('--date', type=str)
parser.add_argument('--s3_credentials', type=str)
args = parser.parse_known_args()[0]

date_obj = datetime.strptime(args.date, '%Y-%m-%d')

setid = 'HealthVerity_' + \
        date_obj.strftime('%Y%m%d') + \
        (date_obj + timedelta(days=1)).strftime('%m%d')

input_path = 's3://salusv/incoming/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
trunk_path = input_path + 'trunk/'
addon_path = input_path + 'addon/'

matching_path = 's3://salusv/matching/payload/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
output_path = 's3://salusv/warehouse/text/labtests/quest/'

# create helper tables
subprocess.call('psql dev < ' + get_rel_path(
    'create_helper_tables.sql'
), shell=True)

# create date table
date_validator.generate(args.s3_credentials)

# create table for lab common model
prov_id_hash = hashlib.md5()
prov_id_hash.update("18")
prov_hash = hashlib.md5()
prov_hash.update("quest")
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'filename="\'' + setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'' + prov_id_hash.hexdigest() + '\'"']
    + ['-v', 'vendor="\'' + prov_hash.hexdigest() + '\'"']
    + ['dev', '<', get_rel_path(
        '../../redshift_norm_common/lab_common_model.sql'
    )]
), shell=True)

# load data
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'matching_path="\'' + matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['dev', '<', get_rel_path(
        'load_matching_payload.sql'
    )]
), shell=True)

# Quest data must be ingested differently based on when it was sent -
# they changed the format on 2016/08/31
if args.period == 'current':
    subprocess.call(' '.join(
        ['psql']
        + ['-v', 'trunk_path="\'' + trunk_path + '\'"']
        + ['-v', 'addon_path="\'' + addon_path + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + ['dev', '<', get_rel_path(
            'load_and_merge_transactions.sql'
        )]
    ), shell=True)
elif args.period == 'hist':
    subprocess.call(' '.join(
        ['psql']
        + ['-v', 'input_path="\'' + input_path + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + ['dev', '<', get_rel_path(
            'load_transactions.sql'
        )]
    ), shell=True)
else:
    print("Invalid period '" + args.period + "'")
    exit(1)

# normalize
subprocess.call('psql dev <' + get_rel_path('normalize.sql'), shell=True)

# privacy filtering
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'table_name=lab_common_model']
    + ['-v', 'column_name=diagnosis_code']
    + ['-v', 'qual_column_name=diagnosis_code_qual']
    + ['-v', 'service_date_column_name=date_service']
    + ['dev', '<', get_rel_path(
        '../../redshift_norm_common/nullify_icd9_blacklist.sql'
    )]
), shell=True)
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'table_name=lab_common_model']
    + ['-v', 'column_name=diagnosis_code']
    + ['-v', 'qual_column_name=diagnosis_code_qual']
    + ['-v', 'service_date_column_name=date_service']
    + ['dev', '<', get_rel_path(
        '../../redshift_norm_common/nullify_icd10_blacklist.sql'
    )]
), shell=True)
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'table_name=lab_common_model']
    + ['-v', 'column_name=diagnosis_code']
    + ['-v', 'qual_column_name=diagnosis_code_qual']
    + ['-v', 'service_date_column_name=date_service']
    + ['dev', '<', get_rel_path(
        '../../redshift_norm_common/genericize_icd9.sql'
    )]
), shell=True)
subprocess.call(' '.join(
    ['psql']
    + ['-v', 'table_name=lab_common_model']
    + ['-v', 'column_name=diagnosis_code']
    + ['-v', 'qual_column_name=diagnosis_code_qual']
    + ['-v', 'service_date_column_name=date_service']
    + ['dev', '<', get_rel_path(
        '../../redshift_norm_common/genericize_icd10.sql'
    )]
), shell=True)

# unload to s3
year_months = subprocess.check_output(' '.join(
    ['psql']
    + ['dev', '-c', '\'select distinct substring(date_service, 0, 8) from lab_common_model\'']
), shell=True)

for ym in map(
        lambda x: x.strip(),
        year_months.decode('utf-8').split('\n')[2:-3]
):
    subprocess.call(' '.join(
        ['psql']
        + ['-v', 'output_path="\'' + output_path + ym + '/\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + ['-v', 'select_from_common_model_table="\'SELECT * FROM lab_common_model WHERE substring(date_service, 0, 8) =  \\\'' + ym + '\\\'\'"']
        + ['dev', '<', get_rel_path(
            '../../redshift_norm_common/unload_common_model.sql'
        )]
    ), shell=True)
