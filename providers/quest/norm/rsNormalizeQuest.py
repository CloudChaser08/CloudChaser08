#! /usr/bin/python
import subprocess
import argparse
import time
import sys
import os
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--method', type=str)
parser.add_argument('--input_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str)
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
args = parser.parse_args()

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
subprocess.call(' '.join(
    psql
    + ['-v', 'filename="\'' + args.setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'quest lab\'"']
    + ['-v', 'vendor="\'quest\'"']
    + [db, '<', '../redshift_norm_common/lab_common_model.sql']
), shell=True)

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# Quest data must be ingested differently based on when it was sent -
# they changed the format on 2016/08/31
if args.period is 'current':
    subprocess.call(' '.join(
        psql
        + ['-v', 'trunk_path="\'' + args.input_path + 'trunk/' + '\'"']
        + ['-v', 'addon_path="\'' + args.input_path + 'addon/' + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'load_and_merge_transactions.sql']
    ), shell=True)
elif args.period is 'hist':
    subprocess.call(' '.join(
        psql
        + ['-v', 'input_path="\'' + args.input_path + '\'"']
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
    [db, '<', '../redshift_norm_common/nullify_icd9_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../redshift_norm_common/nullify_icd10_blacklist.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../redshift_norm_common/genericize_icd9.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=lab_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'qual_column_name=diagnosis_code_qual'] +
    [db, '<', '../redshift_norm_common/genericize_icd10.sql']), shell=True)

# unload to s3
subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM lab_common_model\'"']
    + [db, '<', '../redshift_norm_common/unload_common_model.sql']
), shell=True)

# # # unload patient hlls
# # subprocess.call(' '.join(
# #     psql
# #     + ['-v', 'output_path="\'' + args.output_path + '/testing_script/diagnosis/' + '\'"']
# #     + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
# #     + ['-v', 'select_for_hvid="\'SELECT diagnosis_code, hvid FROM lab_common_model\'"']
# #     + [db, '<', 'unload_hlls.sql']
# # ), shell=True)
# # subprocess.call(' '.join(
# #     psql
# #     + ['-v', 'output_path="\'' + args.output_path + '/testing_script/gender/' + '\'"']
# #     + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
# #     + ['-v', 'select_for_hvid="\'SELECT patient_gender, hvid FROM lab_common_model\'"']
# #     + [db, '<', 'unload_hlls.sql']
# # ), shell=True)
# # subprocess.call(' '.join(
# #     psql
# #     + ['-v', 'output_path="\'' + args.output_path + '/testing_script/age/' + '\'"']
# #     + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
# #     + ['-v', 'select_for_hvid="\'SELECT patient_age, hvid FROM lab_common_model\'"']
# #     + [db, '<', 'unload_hlls.sql']
# # ), shell=True)
# # subprocess.call(' '.join(
# #     psql
# #     + ['-v', 'output_path="\'' + args.output_path + '/testing_script/state/' + '\'"']
# #     + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
# #     + ['-v', 'select_for_hvid="\'SELECT patient_state, hvid FROM lab_common_model\'"']
# #     + [db, '<', 'unload_hlls.sql']
# # ), shell=True)

