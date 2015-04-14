#!/bin/sh

echo "Loading Diagnosis Codes..."
psql -U $1 -d hvdb -c "COPY diagnosis_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')" < $2

echo "Loading Procedure Codes..."
psql -U $1 -d hvdb -c "COPY procedure_codes (code, long_description, short_description) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')" < $3



