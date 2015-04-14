#!/bin/sh

echo "Loading Diagnosis Codes..."
sed -e 's/,/\n/g' $2 | sed -e 's/,$//g' | psql -U $1 -d hvdb -c "COPY diagnosis_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', encoding 'windows-1251')" 

echo "Loading Procedure Codes..."
sed -e 's/,/\n/g' $3 | sed -e 's/,$//g' | psql -U $1 -d hvdb -c "COPY procedure_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', encoding 'windows-1251')" 



