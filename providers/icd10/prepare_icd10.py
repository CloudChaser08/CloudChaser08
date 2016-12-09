#!/usr/bin/python
# 12-8-2016
# healthverity
# kyle halpin

import os
import sys
import struct
import getopt

def print_help():
    print("Updates icd10 codes, takes existing code file, adds new codes")
    print("Example: prepare_icd10.py -o icd10pcs_order_2015.txt -n icd10pcs_order_2016.txt -u outfile")

def get_lines(file_name):
    # load files into their dicts, key on code
    lines = {}
    fd = open(file_name,'r')
    for line in fd:
        num,code = struct.unpack("5s 1x 7s",line[:13])
        lines[code] = line
    fd.close
    return lines

def format_and_save(lines,file_name):
    f = open(file_name,'w')
    fmt_string = '5s 1x 7s 1x 1s 1x 60s'
    for key in lines:
        # unpack what we know is an adequate length string
        s1, s2, s3, s4 = struct.unpack(fmt_string,lines[key][:76])
        # grab whatever is left
        s5 = lines[key][77:]
        # print it out
        # tab style
        #f.write(s1+"\t"+s2+"\t"+s3+"\t"+s4+"\t"+s5)
        # input file style (needed for next year)
        f.write("{:5} {:7} {:1} {:60} {}".format(s1,s2,s3,s4,s5))
        #print("{:5} {:7} {:1} {:60} {}".format(s1,s2,s3,s4,s5))

    f.close()

def main():
    codes_added = 0
    codes_modified = 0
    codes_removed = []
    options, remainder = getopt.getopt(sys.argv[1:], 'o:n:h:du:')
    for opt, arg in options:
        if opt in ('-o'):
            old_file_name = arg
        elif opt in ('-n'):
            new_file_name = arg
        elif opt in ('-u'):
            updated_file_name = arg
        elif opt in ('-h'):
            print_help = True

    # read the files in
    old_lines = get_lines(old_file_name)
    new_lines = get_lines(new_file_name)

    # first, find diffs, update to new one if found
    for code in old_lines:
        if code not in new_lines:
            codes_removed.append(code)
            continue
        if(old_lines[code] != new_lines[code]):
            print("Line with code "+code+" differs, updating")
            print("Old line is "+old_lines[code])
            print("New line is "+new_lines[code])
            old_lines[code] = new_lines[code]
            codes_modified = codes_modified+1

    # next, find new codes that do not exist in old codes, add to old codes
    for code in new_lines:
        if code not in old_lines:
            print("Code "+code+" does not exist, adding")
            old_lines[code] = new_lines[code]
            codes_added = codes_added+1

    format_and_save(old_lines,updated_file_name)
    print("Complete, added "+str(codes_added)+", updated "+str(codes_modified)+" removed "+str(len(codes_removed)))
    print("Codes removed, but we didn't actually remove them:")
    print(codes_removed)

if __name__ == "__main__":
    main()
