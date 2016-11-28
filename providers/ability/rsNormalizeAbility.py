#! /usr/bin/python
import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--header_path', type=str)
parser.add_argument('--serviceline_path', type=str)
parser.add_argument('--servicelineaffiliation_path', type=str)
parser.add_argument('--diagnosis_path', type=str)
parser.add_argument('--procedure_path', type=str)
parser.add_argument('--billing_path', type=str)
parser.add_argument('--payer_path', type=str)
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

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# create date table
from datetime import timedelta, date, datetime

subprocess.call(' '.join(
    psql + [db, '-c', '"DROP TABLE IF EXISTS dates"']
), shell=True)
subprocess.call(' '.join(
    psql + [db, '-c', '"CREATE TABLE dates (date text encode lzo, formatted text encode lzo) DISTSTYLE ALL"']
), shell=True)

start_date = date(2013, 1, 1)
end_date = datetime.now().date()
date_range = [start_date + timedelta(n) for n in range(int ((end_date - start_date).days))]

with open('temp.csv','w') as output:
    for single_date in date_range:
        output.write(single_date.strftime("%Y%m%d") + ',' + single_date.strftime("%Y-%m-%d") + '\n')

subprocess.call('aws s3 cp temp.csv s3://healthveritydev/musifer/practice-insight_normalization/', shell=True)

subprocess.call(' '.join(
    psql + [db, '-c', '"COPY dates FROM \'s3://healthveritydev/musifer/practice-insight_normalization/temp.csv\' CREDENTIALS \'' + args.s3_credentials + '\' FORMAT AS CSV;"']
), shell=True)

# create table for medical claims common model
subprocess.call(' '.join(
    psql
    + ['-v', 'filename="\'' + args.setid + '\'"']
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'feedname="\'practice insight medical claims\'"']
    + ['-v', 'vendor="\'practice insight\'"']
    + [db, '<', '../redshift_norm_common/medicalclaims_common_model.sql']
), shell=True)

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'header_path="\'' + args.header_path + '\'"']
    + ['-v', 'serviceline_path="\'' + args.serviceline_path + '\'"']
    + ['-v', 'servicelineaffiliation_path="\'' + args.servicelineaffiliation_path + '\'"']
    + ['-v', 'diagnosis_path="\'' + args.diagnosis_path + '\'"']
    + ['-v', 'billing_path="\'' + args.billing_path + '\'"']
    + ['-v', 'payer_path="\'' + args.payer_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_transactions.sql']
), shell=True)
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# # explode all diagnosis codes
# subprocess.call(' '.join(
#     psql + [db, '<', 'create_exploded_diagnosis_map.sql']
# ), shell=True)

# # explode all procedure codes
# subprocess.call(' '.join(
#     psql + [db, '<', 'create_exploded_procedure_map.sql']
# ), shell=True)

# # normalize
# subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True) 

# # privacy filtering
# subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
#     ['-v', 'column_name=diagnosis_code'] +
#     ['-v', 'service_date=date_service'] +
#     [db, '<', '../redshift_norm_common/nullify_icd9_blacklist_noqual.sql']), shell=True)
# subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
#     ['-v', 'column_name=diagnosis_code'] +
#     ['-v', 'service_date=date_service'] +
#     [db, '<', '../redshift_norm_common/nullify_icd10_blacklist_noqual.sql']), shell=True)
# subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
#     ['-v', 'column_name=diagnosis_code'] +
#     ['-v', 'service_date=date_service'] +
#     [db, '<', '../redshift_norm_common/genericize_icd9_noqual.sql']), shell=True)
# subprocess.call(' '.join(psql + ['-v', 'table_name=medicalclaims_common_model'] +
#     ['-v', 'column_name=diagnosis_code'] +
#     ['-v', 'service_date=date_service'] +
#     [db, '<', '../redshift_norm_common/genericize_icd10_noqual.sql']), shell=True)
# subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_place_of_service.sql']), shell=True)
# subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/scrub_discharge_status.sql']), shell=True)
# subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/nullify_drg_blacklist.sql']), shell=True)

# # unload to s3
# subprocess.call(' '.join(
#     psql
#     + ['-v', 'output_path="\'' + args.output_path + '\'"']
#     + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
#     + ['-v', 'select_from_common_model_table="\'SELECT * FROM medicalclaims_common_model\'"']
#     + [db, '<', '../redshift_norm_common/unload_common_model.sql']
# ), shell=True)
