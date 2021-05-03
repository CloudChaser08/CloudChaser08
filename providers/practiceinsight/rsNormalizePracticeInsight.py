#! /usr/bin/python
import subprocess
import argparse
import time
import hashlib
import sys
import os
sys.path.append(os.path.abspath("../redshift_norm_common/"))
import create_date_validation_table as date_validator

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str)
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
args = parser.parse_known_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# create helper tables
subprocess.call(' '.join(
    psql + [db, '<', 'create_helper_tables.sql']
), shell=True)

# create date table
date_validator.generate(args.s3_credentials)

# create table for medical claims common model
prov_id_hash = hashlib.md5()
prov_id_hash.update("22")
subprocess.call(' '.join(
    psql
    + ['-v', 'filename="\'' + args.setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'' + prov_id_hash.hexdigest() + '\'"']
    + ['-v', 'vendor="\'practice insight\'"']
    + [db, '<', '../redshift_norm_common/medicalclaims_common_model.sql']
), shell=True)

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'input_path="\'' + args.input_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_transactions.sql']
), shell=True)
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# explode all diagnosis codes
subprocess.call(' '.join(
    psql + [db, '<', 'create_exploded_diagnosis_map.sql']
), shell=True)

# explode all procedure codes
subprocess.call(' '.join(
    psql + [db, '<', 'create_exploded_procedure_map.sql']
), shell=True)

# normalize
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

# privacy filtering
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'service_date=date_service'] +
    [db, '<', '../redshift_norm_common/nullify_icd9_blacklist_noqual.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'service_date=date_service'] +
    [db, '<', '../redshift_norm_common/nullify_icd10_blacklist_noqual.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'service_date=date_service'] +
    [db, '<', '../redshift_norm_common/genericize_icd9_noqual.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
    ['-v', 'column_name=diagnosis_code'] +
    ['-v', 'service_date=date_service'] +
    [db, '<', '../redshift_norm_common/genericize_icd10_noqual.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_place_of_service.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_discharge_status.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/nullify_drg_blacklist.sql']), shell=True)

# unload to s3
subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model\'"']
    + [db, '<', '../redshift_norm_common/unload_common_model.sql']
), shell=True)
