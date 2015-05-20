
CREATE TABLE inovalon_claim_raw (
  claim_uid                               CHAR(11),
  member_uid                              VARCHAR(10),
  provider_uid                            VARCHAR(10),
  claim_status_code                       VARCHAR(1),
  service_date                            DATE,
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
  claim_uid				CHAR(11),
  member_uid				VARCHAR(10),
  service_date				DATE,
  service_thru_date		        DATE,
  code_type				VARCHAR(64),
  ordinal_position			VARCHAR(64),
  code_value                            VARCHAR(64),
  derived                               BOOLEAN
);

CREATE TABLE inovalon_claim_code (
  claim_uid				CHAR(11),
  member_uid				VARCHAR(10),
  service_date				DATE,
  service_thru_date		        DATE,
  ordinal_position			VARCHAR(64),
  pos                                   SMALLINT,
  diagnosis_code                        CHAR(7),
  procedure_code                        CHAR(6),
  procedure_modfier                     CHAR(6),
  procedure_taxonomy                    CHAR(6)
);

CREATE TABLE inovalon_enrollment (
  member_uid                            VARCHAR(10),
  effective_date                        DATE,
  termination_date                      Date,

MemberUID	EffectiveDate	TerminationDate	PayerGroupCode	PayerTypeCode	ProductCode	MedicalInd	RxInd	SourceID
