COPY raw_icd10dx_codes (ordernum, code, header, short_description, long_description)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
DELIMITER '\t'
