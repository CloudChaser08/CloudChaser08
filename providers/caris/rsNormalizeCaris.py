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

# subprocess.call(' '.join(psql + [db, '<', '../redshift_norm_common/zip3_to_state.sql']), shell=True)
subprocess.call(' '.join(psql + ['-v', 'credentials="\'' + args.s3_credentials + '\'"'] +
    [db, '<', 'load_transactions.sql']), shell=True)
subprocess.call(' '.join(psql + [db, '<', 'normalize.sql']), shell=True)

# unload data
# patient
# gender
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/gender/\'"']
    + ['-v', 'select_statement="\'select distinct gender,hvid from full_exploded_payload where gender != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# state
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/state/\'"']
    + ['-v', 'select_statement="\'select distinct state, hvid from full_exploded_payload where state != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# age
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/age/\'"']
    + ['-v', 'select_statement="\'select distinct case when age between 0 and 18 then \\\'0-17\\\' when age between 18 and 45 then \\\'18-44\\\' when age between 45 and 65 then \\\'45-64\\\' when age >= 65 then \\\'65+\\\' else null end as age, hvid from full_exploded_payload where age != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# total
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/total/\'"']
    + ['-v', 'select_statement="\'select distinct \\\'caris\\\',hvid from full_exploded_payload\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# record
# gender
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/gender/\'"']
    + ['-v', 'select_statement="\'select gender,pk from full_exploded_payload where gender != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# state
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/state/\'"']
    + ['-v', 'select_statement="\'select state, pk from full_exploded_payload where state != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# age
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/age/\'"']
    + ['-v', 'select_statement="\'select case when age between 0 and 18 then \\\'0-17\\\' when age between 18 and 45 then \\\'18-44\\\' when age between 45 and 65 then \\\'45-64\\\' when age >= 65 then \\\'65+\\\' else null end as age, pk from full_exploded_payload where age != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# total
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/total/\'"']
    + ['-v', 'select_statement="\'select \\\'caris\\\',pk from full_exploded_payload\'"']
    + [db, '<', 'unload.sql']
), shell=True)
