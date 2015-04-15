


CREATE TABLE ccs_multi_diagnosis_codes (
  diagnosis_code VARCHAR(6),
  level_1    VARCHAR(8),
  level_1_label VARCHAR(255),
  level_2    VARCHAR(8),
  level_2_label VARCHAR(255),
  level_3    VARCHAR(8),
  level_3_label VARCHAR(255),
  level_4    VARCHAR(8),
  level_4_label VARCHAR(255)
);

CREATE TABLE ccs_multi_procedure_codes (
  diagnosis_code VARCHAR(6),
  level_1    VARCHAR(8),
  level_1_label VARCHAR(255),
  level_2    VARCHAR(8),
  level_2_label VARCHAR(255),
  level_3    VARCHAR(8),
  level_3_label VARCHAR(255),
  level_4    VARCHAR(8),
  level_4_label VARCHAR(255)
);
