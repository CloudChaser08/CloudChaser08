#!/bin/sh

echo "Loading ICD10 Diagnosis Codes..."
awk '{ one=substr($0,1,5); two=substr($0,7,7); three=substr($0,15,1); four=substr($0,17,60); five=substr($0,78,400); printf ("%s\t%s\t%s\t%s\t%s", one, two, three, four, five)}' $2 | psql -U $1 -d hvdb -c "COPY icd10_diagnosis_codes (ordernum, code, header, long_description, short_description) FROM STDIN WITH (FORMAT text)" 

echo "Loading ICD10 Procedure Codes..."
awk '{ one=substr($0,1,5); two=substr($0,7,7); three=substr($0,15,1); four=substr($0,17,60); five=substr($0,78,400); printf ("%s\t%s\t%s\t%s\t%s", one, two, three, four, five)}' $3 | psql -U $1 -d hvdb -c "COPY icd10_procedure_codes (ordernum, code, header, long_description, short_description) FROM STDIN WITH (FORMAT text)" 
