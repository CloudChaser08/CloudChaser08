#! /usr/bin/python
import subprocess
import re
import argparse
import time

res = subprocess.check_output(' '.join(['aws', 's3', 'ls', 's3://salusv/incoming/pharmacyclaims/esi/', '--recursive', '|', 'grep', '-o', 'incoming.*', '|', 'sed', "'s/\.decrypted\...\.bz2//'", '|', 'uniq']), shell=True)

CREDENTIALS='aws_access_key_id=AKIAJE4TFV6ZUIYWG5YQ;aws_secret_access_key=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'

old_setid = None
i = 0
for f in res.split("\n"):
    if len(re.findall('c140[1-7]|c140801', f)) > 0:
        continue
    in_path = 's3://salusv/' + f
    in_path = re.sub('[^/]+$', '', in_path)
    matching_path = re.sub('incoming', 'matching/payload', in_path)
    setid = f.split('/')[-1]
    extra_pieces_path = ('s3://salusv/matching/esi-rx-numbers/payload/'
        'f726747e-9dc0-4023-9523-e077949ae865/' + re.findall('10130X.*c\d{6}', setid)[0]
        )

    args = ['--input_path=\'{}\''.format(in_path),
        '--matching_path=\'{}\''.format(matching_path),
        '--extra_pieces_path=\'{}\''.format(extra_pieces_path),
        '--s3_credentials=\'{}\''.format(CREDENTIALS),
        '--setid=\'{}\''.format(setid)]

    if i == 0:
        args.append('--create_reversal_table')
    else:
        args.append('--unload_setid=\'{}\''.format(old_setid))
        args.append('--output_path=\'{}\''.format(out_path))

    print ' '.join(['./rsNormalizeExpressScriptsRX.py'] + args)
    subprocess.check_call(' '.join(['./rsNormalizeExpressScriptsRX.py'] + args), shell=True)
    time.sleep(5)

    out_path = 's3://healthveritydev/ifishbein/' + f
    out_path = re.sub('[^/]+$', '', out_path)
    out_path = re.sub('incoming', 'processed', out_path)
    old_setid = setid
    i += 1
