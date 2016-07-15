
COPY raw_icd9pcs_codes (code, long_description, short_description)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
CSV
IGNOREHEADER 1
DELIMITER ','
