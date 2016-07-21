#!/usr/bin/env python2.7

import xlrd
import csv
from sys import argv

def xls_to_csv(xlsfilename):
    wb = xlrd.open_workbook(xlsfilename)
    sh = wb.sheet_by_index(0)
    with (open(xlsfilename+'.csv', 'wb')) as csvout:
        wr = csv.writer(csvout, quoting=csv.QUOTE_ALL, lineterminator="\n")

        for rownum in xrange(sh.nrows):
            wr.writerow(map(lambda x: x.encode('utf-8'), sh.row_values(rownum)))

xls_to_csv(argv[1])
