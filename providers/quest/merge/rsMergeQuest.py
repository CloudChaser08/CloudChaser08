import subprocess
import argparse
import time
from datetime import date, timedelta, datetime

TODAY = time.strftime('%Y-%m-%d', time.localtime())

parser = argparse.ArgumentParser()
parser.add_argument('--incoming_base', type=str)
parser.add_argument('--output_path', type=str)
parser.add_argument('--cluster_endpoint', type=str)
parser.add_argument('--s3_credentials', type=str)
parser.add_argument('--rs_user', type=str)
parser.add_argument('--rs_password', type=str)
parser.add_argument('--start_date', type=str)
parser.add_argument('--end_date', type=str)
args = parser.parse_known_args()[0]

db = 'dev'
psql = ['psql', '-h', args.cluster_endpoint, '-p', '5439']
if args.rs_user:
    psql.append('-U')
    psql.append(args.rs_user)

# we only need to merge this date range - [2014-09-01 - 2016-08-31)
# use start date and end date args if they exist
start_date = datetime.strptime(args.start_date, '%Y-%m-%d') \
             if args.start_date else date(2014, 9, 1)
end_date = datetime.strptime(args.end_date, '%Y-%m-%d') \
           if args.end_date else date(2016, 8, 31)
date_range = [
    start_date + timedelta(n) for n in range(int((end_date - start_date).days))
]

for d in date_range:
    # how quest indicates the current date
    formatted_date = d.strftime('%Y%m%d') + (d + timedelta(1)).strftime('%m%d')

    print('Merging ' + formatted_date)

    base = args.incoming_base if args.incoming_base.endswith('/') \
        else args.incoming_base + '/'
    trunk_path = base + 'HealthVerity_' + formatted_date + '_2.gz'
    output = (
        args.output_path if args.output_path.endswith('/')
        else args.output_path + '/'
    ) + str(d.year) + '/' + str(d.month).zfill(2) + '/' + str(d.day).zfill(2) + '/'

    # import originals
    subprocess.call(' '.join(
        psql
        + ['-v', 'input_path="\'' + trunk_path + '\'"']
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + [db, '<', 'copy.sql']
    ), shell=True)

    # only need to do this if we're starting a new year
    if (
            d.year == 2014 and formatted_date[4:8] == '0901'
    ) or formatted_date[4:8] == '0101':
        print('New year! Recreating addon and lab tables')

        addon_path = base + 'unzipped/HVRetro' + str(d.year)
        lab_path = base + 'unzipped/HealthVerity_Lab_Retro' + str(d.year)

        # import addons
        subprocess.call(' '.join(
            psql
            + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
            + ['-v', 'input_path="\'' + addon_path + '\'"']
            + [db, '<', 'copy_retro.sql']
        ), shell=True)

        # unique addons
        subprocess.call(' '.join(
            psql + [db, '<', 'unique_retro.sql']
        ), shell=True)

        # import lab
        subprocess.call(' '.join(
            psql
            + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
            + ['-v', 'input_path="\'' + lab_path + '\'"']
            + [db, '<', 'copy_lab.sql']
        ), shell=True)

        # unique lab
        subprocess.call(' '.join(
            psql + [db, '<', 'unique_lab.sql']
        ), shell=True)

    # merge
    subprocess.call(' '.join(
        psql + [db, '<', 'create_merged.sql']
    ), shell=True)
    subprocess.call(' '.join(
        psql + [db, '<', 'merge.sql']
    ), shell=True)

    # unload
    subprocess.call(' '.join(
        psql
        + ['-v', 'credentials="\'' + args.s3_credentials + '\'"']
        + ['-v', 'output_path="\'' + output + '\'"']
        + ['-v', 'select_from_common_model_table="\''
           + 'select * from quest_merged_new\'"']
        + [db, '<', '../../redshift_norm_common/unload_common_model.sql']
    ), shell=True)
