COPY icd10_diagnosis_codes (ordernum, code, header, short_description, long_description)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
DELIMITER '\t'
