#! /usr/bin/python
import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--input_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--setid', type=str, nargs='?')
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
    psql.extend(['-U', args.rs_user])

subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/zip3_to_state.sql']), shell=True)

subprocess.call(' '.join(psql + ['-v', 'filename="\'' + args.setid + '\'"'] + 
    ['-v', 'today="\'' + TODAY + '\'"'] +
    ['-v', 'feedname="\'caris lab tests\'"'] +
    ['-v', 'vendor="\'caris\'"'] +
    [db, '<', '../redshift_norm_common/lab_common_model.sql']), shell=True)

subprocess.call(' '.join(psql + ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    ['-v', 'input_path="\'' + args.input_path + '\'"'] +
    ['-v', 'matching_path="\'' + args.matching_path + '\'"'] +
    [db, '<', 'load_transactions.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)
