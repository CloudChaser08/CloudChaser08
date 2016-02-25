

CREATE TABLE ndc_package (
  product_id VARCHAR(64),
  product_ndc VARCHAR(10),
  ndc_package_code VARCHAR(12),
  package_description VARCHAR(1024)
);


CREATE TABLE ndc_product (
  product_id VARCHAR(64),
  product_ndc VARCHAR(10),
  product_type VARCHAR(64),
  proprietary_name VARCHAR(1024),
  proprietary_name_suffix VARCHAR(512),
  nonproprietary_name VARCHAR(1024),
  dosage_form_name VARCHAR(64),
  route_name VARCHAR(128),
  start_marketing_date VARCHAR(10),
  end_marketing_date VARCHAR(10),
  marketing_category_name VARCHAR(64),
  application_number VARCHAR(32),
  labeler_name VARCHAR(512),
  substance_name VARCHAR(4096),
  active_numerator_strength VARCHAR(1024),
  active_ingred_unit VARCHAR(4096),
  pharm_classes VARCHAR(4096),
  dea_schedule VARCHAR(8)
);
