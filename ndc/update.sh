#!/bin/sh

psql -U $0 -d hvdb -c "COPY ndc_product (product_id, product_ndc, ndc_package_code, package_description) FROM STDIN DELIMITER ',' CSV HEADER" < $1
