#!/bin/sh

echo "Loading HCPCs..."
sed -e 's/,/\n/g' $2 | sed -e 's/,$//g' | psql -U $1 -d hvdb -c "COPY hcpcs (hcpc, long_description, short_description, price_cd1, price_cd2, price_cd3, price_cd4, multi_pi, cim1, cim2, cim3, mcm1, mcm2, mcm3, statute, lab_cert_cd1, lab_cert_cd2, lab_cert_cd3, lab_cert_cd4, lab_cert_cd5, lab_cert_cd6, lab_cert_cd7, lab_cert_cd8, xref1, xref2, xref3, xref4, xref5, cov_code, asc_gpcd, asc_eff_dt, proc_note, betos, tos1, tos2, tos3, tos4, tos5, anes_unit, add_date, act_eff_dt, term_dt, action_code) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', encoding 'windows-1251')" 



