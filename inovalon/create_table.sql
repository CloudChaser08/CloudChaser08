
CREATE TABLE inovalon_claim_raw (
  claim_uid                               CHAR(11) distkey,
  member_uid                              VARCHAR(10),
  provider_uid                            VARCHAR(10),
  claim_status_code                       VARCHAR(1),
  service_date                            DATE sortkey,
  service_thru_date                       DATE,
  ub_patient_discharge_status_code        CHAR(2),
  service_unit_quantity                   INTEGER,
  denied_days_count                       INTEGER,
  billed_amount                           FLOAT,
  allowed_amount                          FLOAT,
  copay_amount                            FLOAT,
  paid_amount                             FLOAT,
  cost_amount                             FLOAT,
  rx_provider                             BOOLEAN,
  pcp_provider                            BOOLEAN,
  room_board                              BOOLEAN,
  major_surgery                           BOOLEAN,
  exclude_discharge                       BOOLEAN
);


CREATE TABLE inovalon_claim_code_raw (
  claim_uid                             CHAR(11) distkey,
  member_uid                            VARCHAR(10),
  service_date                          DATE sortkey,
  service_thru_date                     DATE,
  code_type                             VARCHAR(64),
  ordinal_position                      VARCHAR(64),
  code_value                            VARCHAR(64),
  derived                               BOOLEAN
);

CREATE TABLE inovalon_claim_code (
  claim_uid                             CHAR(11),
  member_uid                            VARCHAR(10),
  service_date                          DATE,
  service_thru_date                     DATE,
  ordinal_position                      VARCHAR(64),
  pos                                   SMALLINT,
  diagnosis_code                        CHAR(7),
  procedure_code                        CHAR(6),
  procedure_modfier                     CHAR(6),
  procedure_taxonomy                    CHAR(6)
);

CREATE TABLE inovalon_enrollment (
  member_uid                            VARCHAR(10),
  effective_date                        DATE,
  termination_date                      DATE,
  payer_group_code                      CHAR(1),
  payer_type_code                       VARCHAR(2),
  product_code                          CHAR(1),
  medical                               BOOLEAN,
  rx                                    BOOLEAN,
  source_id                             INTEGER
);

CREATE TABLE inovalon_member (
  member_uid                            VARCHAR(10),
  birth_year                            CHAR(4),
  gender                                CHAR(1),
  state                                 CHAR(2),
  zip3                                  VARCHAR(50),
  ethnicity                             CHAR(2)
);

CREATE TABLE inovalon_labclaim (
  labclaim_uid                          BIGINT,
  member_uid                            VARCHAR(10),
  provider_uid                          VARCHAR(10),
  claim_status_code                     VARCHAR(1),
  service_date                          DATE,
  cpt_code                              CHAR(5),
  loinc_code                            CHAR(7),
  result_number                         FLOAT,
  result_text                           VARCHAR(50),
  pos_neg_result                        BOOLEAN,
  unit_name                             VARCHAR(20)
);

CREATE TABLE inovalon_provider (
  provider_uid                          VARCHAR(10),
  last_name                             VARCHAR(50),
  first_name                            VARCHAR(50),
  middle_name                           VARCHAR(50),
  company                               VARCHAR(100),
  npi1                                  VARCHAR(10),
  npi1_type_code                        CHAR(1),
  parent_org_1                          VARCHAR(100),
  npi2                                  VARCHAR(10),
  npi2_type_code                        CHAR(1),
  parent_org_2                          VARCHAR(100),
  practice_address_primary              VARCHAR(50),
  practice_address_secondary            VARCHAR(50),
  practice_city                         VARCHAR(28),
  practice_state                        CHAR(2),
  practice_zip                          CHAR(5),
  practice_zip4                         CHAR(4),
  practice_phon                         VARCHAR(10),
  billing_address_primary               VARCHAR(50),
  billing_address_secondary             VARCHAR(50),
  billing_city                          VARCHAR(29),
  billing_state                         CHAR(2),
  billing_zip                           CHAR(5),
  billing_zip4                          CHAR(4),
  billing_phone                         VARCHAR(10),
  taxonomy_code_1                       VARCHAR(10),
  taxonomy_type_1                       VARCHAR(100),
  taxonomy_classification_1             VARCHAR(100),
  taxonomy_specialization_1             VARCHAR(100),
  taxonomy_code_2                       VARCHAR(10),
  taxonomy_type_2                       VARCHAR(100),
  taxonomy_classification_2             VARCHAR(100),
  taxonomy_specialization_2             VARCHAR(100)
);


CREATE TABLE inovalon_rxclaim (
  claim_uid                             CHAR(11),
  member_uid                            VARCHAR(10),
  provider_uid                          VARCHAR(10),
  claim_status_code                     VARCHAR(1),
  fill_date                             DATE,     
  ndc11_code                            VARCHAR(11),
  supply_days_count                     INTEGER,
  dispense_quantity                     FLOAT,
  billed_amount                         NUMERIC(19,4),
  allowed_amount                        NUMERIC(19,4),
  copay_amount                          NUMERIC(19,4),
  paid_amount                           NUMERIC(19,4),
  cost_amount                           NUMERIC(19,4)
);

CREATE TABLE inovalon_provider_supplemental (
  provider_uid                          VARCHAR(10),
  prefix                                VARCHAR(15),
  name                                  VARCHAR(100),
  suffix                                VARCHAR(15),
  address1                              VARCHAR(100),
  address2                              VARCHAR(100),
  city                                  VARCHAR(50),
  state                                 CHAR(2),
  zip                                   VARCHAR(10),
  phone                                 VARCHAR(20),
  fax                                   VARCHAR(20),
  dea_number                            VARCHAR(9),
  npi_number                            VARCHAR(10)
);
