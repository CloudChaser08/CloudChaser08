#! /usr/bin/python
import subprocess
import re
import argparse
import time

res = subprocess.check_output(' '.join(['aws', 's3', 'ls', 's3://salusv/incoming/pharmacyclaims/emdeon/', '--recursive', '|', 'grep', '-o', 'incoming.*', '|', 'sed', "'s/\...\.bz2//'", '|', 'uniq']), shell=True)

CREDENTIALS='aws_access_key_id=AKIAJE4TFV6ZUIYWG5YQ;aws_secret_access_key=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'

old_setid = None
i = 0
unload_queue = []
for f in res.split("\n"):
    if len(re.findall('2015/1./..|2016/../..', f)) == 0:
        continue
    in_path = 's3://salusv/' + f
    in_path = re.sub('[^/]+$', '', in_path)
    matching_path = re.sub('incoming', 'matching/payload', in_path)
    setid = f.split('/')[-1]

    matching_path = 's3://salusv/' + subprocess.check_output(' '.join(['aws', 's3', 'ls', '--recursive',
        matching_path, '|', 'grep', '-o', '\'matching.*.lbz.bz2$\'']), shell=True).strip()

    args = ['--input_path=\'{}\''.format(in_path),
        '--matching_path=\'{}\''.format(matching_path),
        '--s3_credentials=\'{}\''.format(CREDENTIALS),
        '--setid=\'{}\''.format(setid)]

    if i == 0:
        args.append('--create_reversal_table')
        args.append('--first_run')
    if len(unload_queue) == 14:
        to_unload = unload_queue.pop(0)
        args.append('--unload_setid=\'{}\''.format(to_unload['setid']))
        args.append('--output_path=\'{}\''.format(to_unload['out_path']))

    print ' '.join(['./rsNormalizeEmdeonRX.py'] + args)
    subprocess.check_call(' '.join(['./rsNormalizeEmdeonRX.py'] + args), shell=True)
    time.sleep(5)

    out_path = 's3://healthveritydev/ifishbein/' + f
    out_path = re.sub('[^/]+$', '', out_path)
    out_path = re.sub('incoming', 'processed', out_path)
    unload_queue.append({'setid' : setid, 'out_path' : out_path})
    i += 1
