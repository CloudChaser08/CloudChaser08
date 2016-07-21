

CREATE TABLE icd10_diagnosis_codes (
  ordernum VARCHAR(5),
  code VARCHAR(7),
  header CHAR(1),
  short_description VARCHAR(60),
  long_description VARCHAR(512)
);


CREATE TABLE icd10_procedure_codes (
  ordernum VARCHAR(5),
  code VARCHAR(7),
  header CHAR(1),
  short_description VARCHAR(60),
  long_description VARCHAR(512)
);
