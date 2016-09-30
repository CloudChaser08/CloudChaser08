#! /usr/bin/python
import subprocess
import re
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_EMDEON_IN = 's3://salusv/incoming/medicalclaims/emdeon/'
S3_EMDEON_OUT = 's3://salusv/processed/medicalclaims/emdeon/'
S3_EMDEON_MPL = 's3://salusv/matching/payload/medicalclaims/emdeon/'

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str)
parser.add_argument('--cluster_endpoint', type=str, nargs='?')
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
args = parser.parse_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-p', '5439']
if args.cluster_endpoint:
    psql.extend(['-h', args.cluster_endpoint])
if args.rs_user:
    psql.extend(['-U', args.rs_user, '-W'])

subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/zip3_to_state.sql']), shell=True)

# Create a table for valid dates and their correct format
min_date = '2012-01-01'
subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/prep_date_offset_table.sql']), shell=True)
while True:
    res1 = subprocess.check_output(['psql', '-c', 'SELECT count(*) FROM tmp;']);
    res2 = subprocess.check_output(['psql', '-c', 'SELECT extract(\'days\' FROM (getdate() - \'' + min_date + '\'))::int;']);
    rows = int(res1.split("\n")[2].lstrip().rstrip())
    target = int(res2.split("\n")[2].lstrip().rstrip())
    if rows > target + 1:
        break
    else:
        subprocess.call(' '.join(psql + [db, '<', '../../redshift_norm_common/expand_date_offset_table.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'min_valid_date="\'' + min_date + '\'"'] +
    [db, '<', '../../redshift_norm_common/create_date_formatting_table.sql']), shell=True)

subprocess.call(' '.join(psql + ['-v', 'filename="\'' + args.setid + '\'"'] + 
    ['-v', 'today="\'' + TODAY + '\'"'] +
    ['-v', 'feedname="\'webmd medical claims\'"'] +
    ['-v', 'vendor="\'webmd\'"'] +
    [db, '<', '../../redshift_norm_common/pharmacyclaims_common_model.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'input_path="\'' + args.input_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_transactions.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'matching_path="\'' + args.matching_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_matching_payload.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_payer_mapping.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', 'normalize_pharmacy_claims.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'output_path="\'' + args.output_path + '\'"'] +
    ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    ['-v', 'select_from_common_model_table="\'SELECT * FROM pharmacyclaims_common_model\'"'] +
    [db, '<', '../../redshift_norm_common/unload_common_model.sql']), shell=True)
