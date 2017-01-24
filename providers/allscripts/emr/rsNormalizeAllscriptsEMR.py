#! /usr/bin/python
import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--transactional_path', type=str)
parser.add_argument('--matching_path', type=str)
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

transactional_vitals = args.transactional_path + 'Vitals.txt'
transactional_vaccines = args.transactional_path + 'Vaccines.txt'
transactional_results = args.transactional_path + 'Results.txt'
transactional_providers = args.transactional_path + 'Providers.txt'
transactional_problems = args.transactional_path + 'Problems.txt'
transactional_patients = args.transactional_path + 'Patients.txt'
transactional_orders = args.transactional_path + 'Orders.txt'
transactional_medications = args.transactional_path + 'Medications.txt'
transactional_encounters = args.transactional_path + 'Encounters.txt'
transactional_appointments = args.transactional_path + 'Appointments.txt'
transactional_allergies = args.transactional_path + 'Allergies.txt'

# create common model table
subprocess.call(' '.join(
    psql
    + [db, '<', '../../redshift_norm_common/emr_common_model.sql']
), shell=True)

# load transactional data
subprocess.call(' '.join(
    psql
    + ['-v', 'transactional_vaccines_input_path="\'' + transactional_vaccines + '\'"']
    + ['-v', 'transactional_results_input_path="\'' + transactional_results + '\'"']
    + ['-v', 'transactional_providers_input_path="\'' + transactional_providers + '\'"']
    + ['-v', 'transactional_problems_input_path="\'' + transactional_problems + '\'"']
    + ['-v', 'transactional_patients_input_path="\'' + transactional_patients + '\'"']
    + ['-v', 'transactional_orders_input_path="\'' + transactional_orders + '\'"']
    + ['-v', 'transactional_medications_input_path="\'' + transactional_medications + '\'"']
    + ['-v', 'transactional_encounters_input_path="\'' + transactional_encounters + '\'"']
    + ['-v', 'transactional_appointments_input_path="\'' + transactional_appointments + '\'"']
    + ['-v', 'transactional_allergies_input_path="\'' + transactional_allergies + '\'"']
    + ['-v', 'transactional_vitals_input_path="\'' + transactional_vitals + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_transactions.sql']
), shell=True)

# load matching payload
subprocess.call(' '.join(
    psql
    + ['-v', 'matching_path="\'' + args.matching_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'load_matching_payload.sql']
))


# normalize

