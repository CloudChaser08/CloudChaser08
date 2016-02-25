

CREATE TABLE raw_icd10dx_codes (
  ordernum VARCHAR(5),
  code VARCHAR(7),
  header CHAR(1),
  short_description VARCHAR(60),
  long_description VARCHAR(512)
);


CREATE TABLE raw_icd10pcs_codes (
  ordernum VARCHAR(5),
  code VARCHAR(7),
  header CHAR(1),
  short_description VARCHAR(60),
  long_description VARCHAR(512)
);
