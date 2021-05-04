#! /usr/bin/python
import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--transactional_path', type=str)
parser.add_argument('--matching_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str, nargs='?')
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--gz', default=False, action='store_true')
parser.add_argument('--rs_user', type=str, nargs='?')
parser.add_argument('--rs_password', type=str, nargs='?')
args = parser.parse_known_args()[0]

db = args.database if args.database else 'dev'

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

transactional_vitals = args.transactional_path + '/vitals/'
transactional_vaccines = args.transactional_path + '/vaccines/'
transactional_results = args.transactional_path + '/results/'
transactional_providers = args.transactional_path + '/providers/'
transactional_problems = args.transactional_path + '/problems/'
transactional_patients = args.transactional_path + '/patientdemographics/'
transactional_orders = args.transactional_path + '/orders/'
transactional_medications = args.transactional_path + '/medications/'
transactional_encounters = args.transactional_path + '/encounters/'
transactional_appointments = args.transactional_path + '/appointments/'
transactional_allergies = args.transactional_path + '/allergies/'

# create common model table
subprocess.call(' '.join(
    psql
    + ['-v', 'today="\'' + TODAY + '\'"']
    + ['-v', 'filename="\'' + args.transactional_path.split('/')[-1] + '\'"']
    + ['-v', 'feedname="\'' + '25' + '\'"']
    + ['-v', 'vendor="\'' + 'allscripts' + '\'"']
    + [db, '<', '../../redshift_norm_common/emr_common_model.sql']
), shell=True)

# load transactional data
if args.gz:
    load_script = 'load_transactions_gz.sql'
else:
    load_script = 'load_transactions_bz2.sql'

subprocess.call(' '.join(
    psql
    + ['-v', 'transactional_vaccines_input_path="\'' + transactional_vaccines + '\'"']
    + ['-v', 'transactional_results_input_path="\'' + transactional_results + '\'"']
    # + ['-v', 'transactional_providers_input_path="\'' + transactional_providers + '\'"']
    + ['-v', 'transactional_problems_input_path="\'' + transactional_problems + '\'"']
    + ['-v', 'transactional_patients_input_path="\'' + transactional_patients + '\'"']
    + ['-v', 'transactional_orders_input_path="\'' + transactional_orders + '\'"']
    + ['-v', 'transactional_medications_input_path="\'' + transactional_medications + '\'"']
    + ['-v', 'transactional_encounters_input_path="\'' + transactional_encounters + '\'"']
    # + ['-v', 'transactional_appointments_input_path="\'' + transactional_appointments + '\'"']
    # + ['-v', 'transactional_allergies_input_path="\'' + transactional_allergies + '\'"']
    # + ['-v', 'transactional_vitals_input_path="\'' + transactional_vitals + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', load_script]
), shell=True)

# load matching payload
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
), shell=True)

# normalize
subprocess.call(' '.join(
    psql
    + [db, '<', 'normalize.sql']
), shell=True)

# unload
subprocess.call(' '.join(
    psql
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'select_from_common_model_table="\'SELECT * FROM emr_common_model\'"']
    + [db, '<', '../../redshift_norm_common/unload_common_model.sql']
), shell=True)
