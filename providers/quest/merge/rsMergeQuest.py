import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--trunk_path', type=str)
parser.add_argument('--addon_path', type=str)
parser.add_argument('--output_path', type=str)
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

# create tables
subprocess.call(' '.join(
    psql + [db, '<', 'create_tables.sql']
), shell=True)

# import originals
subprocess.call(' '.join(
    psql
    + ['-v', 'input_path="\'' + args.trunk_path + '\'"']
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + [db, '<', 'copy.sql']
), shell=True)

# import addons
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'input_path="\'' + args.addon_path + '\'"']
    + [db, '<', 'copy_retro.sql']
), shell=True)

# merge
subprocess.call(' '.join(psql + [db, '<', 'merge.sql']), shell=True)
