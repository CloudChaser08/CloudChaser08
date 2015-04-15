#!/bin/sh

echo "Loading CCS Diagnosis Codes..."
psql -U $1 -d hvdb -c "COPY ccs_multi_diagnosis_codes (diagnosis_code, level_1, level_1_label, level_2, level_2_label, level_3, level_3_label, level_4) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER true, QUOTE '\'')" < $2

echo "Loading CCS Procedure Codes..."
psql -U $1 -d hvdb -c "COPY ccs_multi_procedure_codes (procedure_code, level_1, level_1_label, level_2, level_2_label, level_3, level_3_label, level_4) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER true, QUOTE '\'')" < $3
