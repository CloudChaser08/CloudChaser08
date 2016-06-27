COPY ndc_product (product_id, product_ndc, product_type, proprietary_name, proprietary_name_suffix, nonproprietary_name, dosage_form_name, route_name, start_marketing_date, end_marketing_date, marketing_category_name, application_number, labeler_name, substance_name, active_numerator_strength, active_ingred_unit, pharm_classes, dea_schedule)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
CSV
IGNOREHEADER 1
ACCEPTINVCHARS
DELIMITER '	'
MAXERROR 20
