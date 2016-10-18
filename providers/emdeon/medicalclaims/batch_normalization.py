#! /usr/bin/python
import subprocess
import re
import argparse
import time

res = subprocess.check_output(' '.join(['aws', 's3', 'ls', 's3://salusv/incoming/medicalclaims/emdeon/', '--recursive', '|', 'grep', '-o', 'incoming.*', '|', 'sed', "'s/\...\.bz2//'", '|', 'uniq']), shell=True)

CREDENTIALS='aws_access_key_id=AKIAJE4TFV6ZUIYWG5YQ;aws_secret_access_key=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'

i = 0
for f in res.split("\n"):
    if len(re.findall('2015/1./..|2016/.[1-5]/..', f)) == 0:
        continue
    in_path = 's3://salusv/' + f
    in_path = re.sub('[^/]+$', '', in_path)
    matching_path = re.sub('incoming', 'matching/payload', in_path)
    setid = f.split('/')[-1]
    extra_pieces_path = ('s3://salusv/matching/emdeon-legacy-matching-engine/payload/'
        '86396771-0345-4d67-83b3-7e22fded9e1d/' + re.sub('.*(201.)/(..)/(..).*', r'\1\2\3_Claims_US_CF_Hash_File', f)
        )

    matching_path = 's3://salusv/' + subprocess.check_output(' '.join(['aws', 's3', 'ls', '--recursive',
        matching_path, '|', 'grep', '-o', '\'matching.*.lbz.bz2$\'']), shell=True).strip()

    out_path = 's3://healthveritydev/ifishbein/' + f
    out_path = re.sub('[^/]+$', '', out_path)
    out_path = re.sub('incoming', 'processed', out_path)

    args = ['--input_path=\'{}\''.format(in_path),
        '--matching_path=\'{}\''.format(matching_path),
        '--extra_pieces_path=\'{}\''.format(extra_pieces_path),
        '--output_path=\'{}\''.format(out_path),
        '--s3_credentials=\'{}\''.format(CREDENTIALS),
        '--setid=\'{}\''.format(setid)]

    print ' '.join(['./rsNormalizeEmdeonDX.py'] + args)
    subprocess.check_call(' '.join(['./rsNormalizeEmdeonDX.py'] + args), shell=True)
    time.sleep(5)
