COPY icd10_procedure_codes (ordernum, code, header, short_description, long_description)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
DELIMITER '\t'
COMPUPDATE ON
