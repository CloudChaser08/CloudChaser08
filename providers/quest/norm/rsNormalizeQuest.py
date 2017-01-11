#! /usr/bin/python
import subprocess
import argparse
import time
import sys
import os
import hashlib
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--method', type=str)
parser.add_argument('--date', type=str)
parser.add_argument('--setid', type=str)
parser.add_argument('--s3_credentials', type=str)
args = parser.parse_args()

input_path = 's3://salusv/incoming/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
trunk_path = input_path + 'trunk/'
addon_path = input_path + 'addon/'

matching_path = 's3://salusv/matching/payload/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)
output_path = 's3://salusv/warehouse/text/labtests/quest/{}/'.format(
    args.date.replace('-', '/')
)

db = args.database if args.database else 'dev'

if args.period not in ['hist', 'current']:
    print('\'period\' argument must be either \'hist\' or \'current\'.')
    exit(1)

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# create helper tables
subprocess.call(' '.join(
    psql + [db, '<', 'create_helper_tables.sql']
), shell=True)

# create date table
date_validator.generate(psql, db, args.s3_credentials)

# create table for lab common model
prov_id_hash = hashlib.md5()
prov_id_hash.update("18")
prov_hash = hashlib.md5()
prov_hash.update("quest")
subprocess.call(' '.join(
    psql
    + ['-v', 'filename="\'' + args.setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'' + prov_id_hash.hexdigest() + '\'"']
    + ['-v', 'vendor="\'' + prov_hash.hexdigest() + '\'"']
    + [db, '<', '../../redshift_norm_common/lab_common_model.sql']
), shell=True)

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# Quest data must be ingested differently based on when it was sent -
# they changed the format on 2016/08/31
if args.period is 'current':
    subprocess.call(' '.join(
        psql
        + ['-v', 'trunk_path="\'' + trunk_path + '\'"']
        + ['-v', 'addon_path="\'' + addon_path + 'addon/' + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_and_merge_transactions.sql']
    ), shell=True)
elif args.period is 'hist':
    subprocess.call(' '.join(
        psql
        + ['-v', 'input_path="\'' + input_path + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_transactions.sql']
    ), shell=True)

# normalize
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

# privacy filtering
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v'
, 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../../redshift_norm_common/nullify_icd9_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../../redshift_norm_common/nullify_icd10_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../../redshift_norm_common/genericize_icd9.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../../redshift_norm_common/genericize_icd10.sql']), shell=True)

# unload to s3
subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM lab_common_model\'"']
    + [db, '<', '../../redshift_norm_common/unload_common_model.sql']
), shell=True)
