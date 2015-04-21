#!/bin/sh

echo "Loading CPT Codes..."
psql -U $1 -d hvdb -c "COPY diagnosis_codes (code, short_description, long_description) FROM STDIN WITH (FORMAT text, encoding 'windows-1251')" < $2



