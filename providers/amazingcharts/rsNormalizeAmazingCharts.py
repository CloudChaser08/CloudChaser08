#! /usr/bin/python
import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--output_path', type=str, nargs='?')
parser.add_argument('--database', type=str, nargs='?')
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

# load data
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_transactional.sql']
), shell=True)

# normalize data
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'normalize.sql']
), shell=True)

# unload data
# patient
# gender
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/gender\'"']
    + ['-v', 'select_statement="\'select distinct patient_gender as gender,hvid from full_transactional where patient_gender != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# state
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/state\'"']
    + ['-v', 'select_statement="\'select distinct patient_state as state, hvid from full_transactional where patient_state != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# age
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/age\'"']
    + ['-v', 'select_statement="\'select distinct patient_age as age,hvid from full_transactional where patient_age != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# diagnosis
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/diagnosis\'"']
    + ['-v', 'select_statement="\'select distinct diagnosis,hvid from full_transactional where diagnosis != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# drug
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/drug\'"']
    + ['-v', 'select_statement="\'select distinct drug,hvid from full_transactional where drug != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# procedure
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/procedure\'"']
    + ['-v', 'select_statement="\'select distinct procedure,hvid from full_transactional where procedure != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# lab
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/lab\'"']
    + ['-v', 'select_statement="\'select distinct lab,hvid from full_transactional where lab != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# total
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/patient/total\'"']
    + ['-v', 'select_statement="\'select distinct \\\'amazingcharts\\\',hvid from full_transactional\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# record
# gender
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/gender\'"']
    + ['-v', 'select_statement="\'select patient_gender as gender, record_id from full_transactional where patient_gender != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# state
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/state\'"']
    + ['-v', 'select_statement="\'select patient_state as state,  record_id from full_transactional where patient_state != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# age
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/age\'"']
    + ['-v', 'select_statement="\'select patient_age as age, record_id from full_transactional where patient_age != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# diagnosis
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/diagnosis\'"']
    + ['-v', 'select_statement="\'select diagnosis, record_id from full_transactional where diagnosis != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# drug
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/drug\'"']
    + ['-v', 'select_statement="\'select drug, record_id from full_transactional where drug != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# procedure
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/procedure\'"']
    + ['-v', 'select_statement="\'select procedure, record_id from full_transactional where procedure != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# lab
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/lab\'"']
    + ['-v', 'select_statement="\'select lab, record_id from full_transactional where lab != \\\'\\\'order by 1\'"']
    + [db, '<', 'unload.sql']
), shell=True)

# total
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '/record/total\'"']
    + ['-v', 'select_statement="\'select \\\'amazingcharts\\\', record_id from full_transactional\'"']
    + [db, '<', 'unload.sql']
), shell=True)
