#!/bin/sh

sed 1d < $2 | psql -U $1 -d hvdb -c "COPY ndc_package (product_id, product_ndc, ndc_package_code, package_description) FROM STDIN WITH (FORMAT text)"
