#!/bin/sh

echo "Loading Diagnosis Codes..."
sed -e 's/^M/\n/g' $2 | awk -F',' '{print $1 "," $2 "," $3}' | psql -U $1 -d hvdb -c "COPY diagnosis_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')" 

echo "Loading Procedure Codes..."
sed -e 's/^M/\n/g' $3 | awk -F',' '{print $1 "," $2 "," $3}' | psql -U $1 -d hvdb -c "COPY procedure_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')" 



