#!/usr/bin/python2.7

import os
from subprocess import check_output as call
from pprint import pprint

COPY = """COPY %s FROM 's3://healthverity/incoming/gsdd/%s'
    WITH CREDENTIALS :'AWS_CREDENTIALS'
    DELIMITER '|'
    TRUNCATECOLUMNS
    COMPUPDATE ON;
"""

path = 'Table/'
listing = os.listdir(path)
for infile in listing:
    head = call('head -1 '+ path + infile, shell=True)
    head = head[:-2]
    head = head[3:]
    head = head.replace(" ",'')
    if not head:
        continue
    cols = head.split('|')
    table = 'gsdd_'+ infile.replace('.txt','')

    drop = "DROP TABLE " + table + ";"
    print drop

    create = "CREATE TABLE %s (\n %s text\n);\n" % (table, " text,\n ".join(cols))
    print create

    copy = COPY % (table,infile)
    print copy
    print "\n\n"

