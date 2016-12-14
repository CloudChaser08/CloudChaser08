#!/usr/bin/python
# 12-8-2016
# healthverity
# kyle halpin

import re
import os
import sys
import csv
import struct
import getopt

def print_help():
    print("Updates NDC codes, takes existing code file, adds new codes")
    print("Example: prepare_ndc.py -o icd10pcs_order_2015.txt -n icd10pcs_order_2016.txt -u outfile")

def get_lines(file_name):
    # load files into their dicts, key on package code
    lines = {}

    field_names = ['PRODUCTID','PRODUCTNDC','NDCPACKAGECODE','PACKAGEDESCRIPTION']

    fd = open(file_name,'r')
    reader = csv.DictReader(fd, delimiter="\t")
    for line in fd:
        reader = csv.DictReader([line],fieldnames=field_names,delimiter="\t")
        for row in reader:
            ndc_package_code = row['NDCPACKAGECODE']
            lines[ndc_package_code] = line

    fd.close
    return lines

def format_and_save(lines,file_name):
    f = open(file_name,'w')
    for code in lines:
        # unpack what we know is an adequate length string
        f.write(lines[code])

    f.close()

def main():
    codes_added = 0
    codes_modified = 0
    codes_removed = []
    options, remainder = getopt.getopt(sys.argv[1:], 'o:n:u:')
    for opt, arg in options:
        if opt in ('-o'):
            old_file_name = arg
        elif opt in ('-n'):
            new_file_name = arg
        elif opt in ('-u'):
            updated_file_name = arg

    # read the files in
    old_lines = get_lines(old_file_name)
    new_lines = get_lines(new_file_name)

    # first, find diffs, update to new one if found
    for code in old_lines:
        if code not in new_lines:
            codes_removed.append(code)
            continue
        if(old_lines[code][47:] != new_lines[code][47:]):
            print("Line with code "+code+" differs, updating")
            print("Old line is "+old_lines[code][47:])
            print("New line is "+new_lines[code][47:])
            old_lines[code] = new_lines[code]
            codes_modified = codes_modified+1

    # next, find new codes that do not exist in old codes, add to old codes
    for code in new_lines:
        if code not in old_lines:
            #print("Code "+code+" does not exist, adding")
            old_lines[code] = new_lines[code]
            codes_added = codes_added+1

    format_and_save(old_lines,updated_file_name)
    print("Complete, added "+str(codes_added)+", updated "+str(codes_modified)+" removed "+str(len(codes_removed)))
    print("Codes removed (but we didn't actually remove them):")
    print(codes_removed)

if __name__ == "__main__":
    main()
