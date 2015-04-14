#!/bin/sh

echo "Loading Package..."
sed 1d < $2 | psql -U $1 -d hvdb -c "COPY ndc_package (product_id, product_ndc, ndc_package_code, package_description) FROM STDIN WITH (FORMAT text)"

echo "Loading Product..." 
sed 1d < $3 | psql -U $1 -d hvdb -c "COPY ndc_product (product_id, product_ndc, product_type, proprietary_name, proprietary_name_suffix, nonproprietary_name, dosage_form_name, route_name, start_marketing_date, end_marketing_date, marketing_category_name, application_number, labeler_name, substance_name, active_numerator_strength, active_ingred_unit, pharm_classes, dea_schedule) FROM STDIN WITH (FORMAT text)"
