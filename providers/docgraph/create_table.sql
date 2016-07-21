
CREATE TABLE docgraph_60day (
  npi_from char(10),
  npi_to char(10),
  shared_transaction_count integer,
  patient_total integer,
  same_day_total integer
);

CREATE TABLE docgraph_procedure (
  npi                             CHAR(10),
  nppes_provider_last_org_name    VARCHAR(100),
  nppes_provider_first_name       VARCHAR(50),
  nppes_provider_mi               VARCHAR(50),
  nppes_credentials               VARCHAR(50),
  nppes_provider_gender           CHAR(1),
  nppes_entity_code               CHAR(1),
  nppes_provider_street1          VARCHAR(100),
  nppes_provider_street2          VARCHAR(100),
  nppes_provider_city             VARCHAR(28),
  nppes_provider_zip              VARCHAR(20),
  nppes_provider_state            CHAR(2),
  nppes_provider_country          CHAR(2),
  provider_type                   VARCHAR(100),
  medicare_participation          BOOLEAN,
  place_of_service                CHAR(1),
  hcpcs_code                      VARCHAR(16),
  line_srvc_cnt                   FLOAT,
  bene_unique_cnt                 INTEGER,
  bene_day_srvc_cnt               INTEGER,
  average_medicare_allowed        NUMERIC(19,4),
  stdev_medicare_allowed          NUMERIC(19,4),
  average_submitted_chrg          NUMERIC(19,4),
  stdev_submitted_chrg            NUMERIC(19,4),
  average_medicare_payment        NUMERIC(19,4),
  stdev_medicare_payment          NUMERIC(19,4)
);
