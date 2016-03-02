
INSERT INTO diag_codes_test (code_type, diag_code, short_desc, long_desc, start_date)
SELECT 'icd9', code, short_description, long_description, now() FROM raw_icd9dx_codes

INSERT INTO diag_codes_test (code_type, diag_code, short_desc, long_desc, start_date)
SELECT 'icd10', code, short_description, long_description, now() FROM raw_icd10dx_codes
