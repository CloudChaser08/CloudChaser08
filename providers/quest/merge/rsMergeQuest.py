import subprocess
import argparse
import time

TODAY = time.strftime('%Y-%m-%d', time.localtime())

#
# Trunk path is on the day level, addon and lab paths are on the year
# level
#
parser = argparse.ArgumentParser()
parser.add_argument('--trunk_path', type=str)
parser.add_argument('--addon_path', type=str)
parser.add_argument('--lab_path', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--database', type=str)
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str)
parser.add_argument('--rs_password', type=str)
parser.add_argument('--new_year', type=bool)
args = parser.parse_args()

db = args.database if args.database else 'dev'

psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# import originals
subprocess.call(' '.join(
    psql
    + ['-v', 'input_path="\'' + args.trunk_manifest + '\'"']
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

# unique addons
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'input_path="\'' + args.addon_path + '\'"']
    + [db, '<', 'unique_retro.sql']
), shell=True)

# import lab
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'input_path="\'' + args.lab_path + '\'"']
    + [db, '<', 'copy_lab.sql']
), shell=True)

# merge
subprocess.call(' '.join(psql + [db, '<', 'create_merged.sql']), shell=True)
subprocess.call(' '.join(
    psql + [db, '<', 'merge.sql']
), shell=True)

# unload
subprocess.call(' '.join(
    psql
    + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
    + ['-v', 'output_path="\'' + args.output_path + '\'"']
    + ['-v', 'select_from_common_model_table="\''
       + 'select * from quest_merged_new\'"']
    + [db, '<', '../../redshift_norm_common/unload_common_model.sql']
))
