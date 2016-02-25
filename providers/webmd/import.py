#!env python2.7

import os
from subprocess import check_output as call
from pprint import pprint

COPY = """COPY %s FROM 's3://healthverity/incoming/webmd/%s'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    ACCEPTINVCHARS
    GZIP
    TRUNCATECOLUMNS
    COMPUPDATE ON;
"""

path = './'
listing = os.listdir(path)
for infile in listing:
    head = call('head -100 ' + path + infile + ' | gunzip -q -c - | head -1 ', shell=True)
    head = head[:-1]
#    head = head[3:]
    head = head.replace(" ",'')
    if not head:
        continue
    cols = head.split('|')
    table = 'webmd_'+ infile.replace('.txt.gz','').replace('SAMPLE_','')

    drop = "DROP TABLE " + table + ";"
    print drop

    create = "CREATE TABLE %s (\n %s text\n);\n" % (table, " text,\n ".join(cols))
    print create

    copy = COPY % (table,infile)
    print copy
    print "\n\n"

