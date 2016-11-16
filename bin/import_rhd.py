#!/usr/bin/python

import psycopg2
import re
import os


try:
    conn = psycopg2.connect("dbname='rhd_test_import' user='hvsuperuser' host='localhost' password='hvsuperuser'")
except Exception as e:
    print("Problem connecting to db")
    print(e)

def load_row(conn,file_name_id,patient_id,body):
    cur = conn.cursor()
    try:
        sql = "INSERT INTO rhd (file_name_id,patient_id,body) VALUES (%s,%s,%s);"
        cur.execute(sql,[file_name_id,patient_id,body])
        conn.commit()
    except Exception as e:
        print("Problem inserting into db")
        print(e)
        os.sys.exit()
    
files = [];
#onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
for f in os.listdir('.'):
    if os.path.isfile(f):
        files.append(f)

for f in files:
    if(f.endswith('.txt')):
        fd = open(f)
        file_name = f.strip('.txt')
        first_line = fd.readline()
        # go back to the start, just in case
        fd.seek(0,0)
        patient_id_match = re.search('^.*\.([0-9]+)',first_line)
        if patient_id_match != None:
                    patient_id = patient_id_match.group(1)
            body = fd.read()
            fd.close()
            load_row(conn,file_name,patient_id,body)
            
        else:
            print("Problem working on "+f)
        
