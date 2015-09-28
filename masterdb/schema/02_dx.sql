
-- DX CLAIMS

CREATE TABLE dx_claims (
    feed_id varchar(64) not null, -- Unique data feed identifier
    claim_id integer not null,    -- Unique claim identifier
    hv_id integer not null,       -- Unique patient identifier
    svc_date_start date not null, -- Date service was initiated for the entire claim
    svc_date_end date not null,   -- Date service was terminated for the entire claim
    date_recd date not null,      -- Date the claim was received by the payer
    payer_id integer,             -- Identifies the payer to which this claim was submitted for reimbursement
    -- XXX types?
    billing_npi varchar(255),     -- Unique NPI for the billing provider
    referring_npi varchar(255),   -- Unique NPI for the referring provider
    facility_npi varchar(255),    -- Unique NPI for the facility which provided care
    total_charge decimal,         -- Total amount billed by provider
    total_allowed decimal         -- Total amount allowed to be billed
);
ALTER TABLE dx_claims OWNER TO hv;

REVOKE ALL ON TABLE dx_claims FROM PUBLIC;
REVOKE ALL ON TABLE dx_claims FROM hv;
GRANT ALL ON TABLE dx_claims TO hv;

-- DX CLAIMS SERVICE

CREATE TABLE dx_claims_svc (
    feed_id varchar(64) not null,        -- Unique data feed identifier
    claim_id integer not null,           -- Unique claim identifier
    hv_id integer not null,              -- Unique patient identifier
    -- XXX types?
    rendering_npi varchar(255) not null, -- Unique physician identifier
    place_of_service varchar(255),       -- Unique place of service code
    svc_date date not null,              -- Date upon which the procedure took place
    svc_line integer not null,           -- Unique service line number for the claim
    diag_code varchar(255) not null,     -- Diagnosis code
    -- XXX types?
    diag_ptr decimal not null,           -- Indicates the priority of the diagnosis code as related to the procedure code
    proc_code varchar(255),              -- Unique procedure code tied to the procedure code
    units varchar(255),                  -- Units administered to the patient
    proc_mod1 varchar(255),              -- Procedure modifier 1
    proc_mod2 varchar(255),              -- Procedure modifier 2
    proc_mod3 varchar(255),              -- Procedure modifier 3
    proc_mod4 varchar(255),              -- Procedure modifier 4
    epsdt varchar(255),                  -- Unique EPSDT (early and periodic screening diagnosis and treatment) code
    line_charge decimal,                 -- Amount billed by the provider for this claim-service line
    line_allowed decimal                 -- Amount allowed to be billed for this claim-service line
);
ALTER TABLE dx_claims_svc OWNER TO hv;

REVOKE ALL ON TABLE dx_claims_svc FROM PUBLIC;
REVOKE ALL ON TABLE dx_claims_svc FROM hv;
GRANT ALL ON TABLE dx_claims_svc TO hv;
