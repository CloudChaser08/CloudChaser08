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
    psql.append('-W')

udf = [x for x in psql]
udf.extend([db, '<', 'udf.sql'])

create_tables = [x for x in psql]
create_tables.extend(['-v', 'filename="\'' + args.setid + '\'"',
        '-v', 'today="\'' + TODAY + '\'"', db, '<', 'create_tables.sql'])

normalize = [x for x in psql]
normalize.extend(['-v', 'credentials="\'' + args.s3_credentials + '\'"',
        '-v', 'input_path="\'' + args.input_path + '\'"',
        '-v', 'output_path="\'' + args.output_path + '\'"',
        '-v', 'matching_path="\'' + args.matching_path + '\'"', db, '<', 'normalize_emdeon_rx.sql'])

subprocess.call(' '.join(create_tables), shell=True)
subprocess.call(' '.join(normalize), shell=True)
