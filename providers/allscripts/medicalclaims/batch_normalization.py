#! /usr/bin/python
import subprocess
import re
import argparse
import time

res = subprocess.check_output(' '.join(['aws', 's3', 'ls', 's3://salusv/incoming/medicalclaims/allscripts/', '--recursive', '|', 'grep', 'header', '|', 'grep', '-o', 'incoming.*', '|', 'sed', "'s/_.\.out\.gz//'", '|', 'uniq']), shell=True)

CREDENTIALS='aws_access_key_id=AKIAJE4TFV6ZUIYWG5YQ;aws_secret_access_key=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'

i = 0
for f in sorted(res.split("\n"), reverse=True):
#    if len(re.findall('2016/0[1-3]/..', f)) == 0:
#        continue
    date = re.search('20[01]./[01]./[0-3].', f).group(0).replace('/', '-')
    setid = f.split('/')[-1]

    args = ['--date=\'{}\''.format(date),
        '--s3_credentials=\'{}\''.format(CREDENTIALS),
        '--setid=\'{}\''.format(setid)]

    if i == 0:
        args.append('--first_run')

    print ' '.join(['./rsNormalizeAllscriptsDX.py'] + args)
    subprocess.check_call(' '.join(['./rsNormalizeAllscriptsDX.py'] + args), shell=True)
    time.sleep(5)
    i += 1
