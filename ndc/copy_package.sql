COPY ndc_package (product_id, product_ndc, ndc_package_code, package_description)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
CSV
IGNOREHEADER 1
DELIMITER '	'
