
-- UB CLAIMS

CREATE TABLE ub_claims (
    feed_id varchar(64) not null,       -- Unique data feed identifier
    claim_id integer not null,          -- Unique claim identifier
    hv_id integer not null,             -- Unique patient identifier
    pat_cntrl varchar(255),             -- Patient control number
    type_bill varchar(255) not null,    -- Type of bill code
    svc_date_start date not null,       -- Date service was initiated for the entire claim
    svc_date_end date not null,         -- Date service was terminated for the entire claim
    date_recd date not null,            -- Date the claim was received by the payer
    admit_date date,                    -- Date patient was admitted to the facility
    admit_type varchar(255),            -- Unique code identifying reason/priority for visit
    admit_source varchar(255),          -- Unique code identifying the point of origin
    disch_status varchar(255) not null, -- Unique code identifying the patient’s status at the time of discharge
    admit_diag varchar(255),            -- Represents the significant reason for admission
    payer_id integer,                   -- Identifies the payer to which this claim was submitted for reimbursement
    -- XXX types?
    billing_npi varchar(255),           -- Unique NPI for the billing provider
    rendering_npi varchar(255),         -- Unique NPI for the provider rendering service
    attending_npi varchar(255),         -- Unique NPI for the attending provider
    operating_npi varchar(255)          -- Unique NPI for the operating provider
);
ALTER TABLE ub_claims OWNER TO hv;

REVOKE ALL ON TABLE ub_claims FROM PUBLIC;
REVOKE ALL ON TABLE ub_claims FROM hv;
GRANT ALL ON TABLE ub_claims TO hv;

-- UB CLAIMS SERVICE

CREATE TABLE ub_claims_svc (
    feed_id varchar(64) not null,   -- Unique data feed identifier
    claim_id integer not null,      -- Unique claim identifier
    hv_id integer not null,         -- Unique patient identifier
    -- XXX types?
    svc_line integer not null,      -- Unique service line number for the claim
    rev_code varchar(255) not null, -- Revenue code
    proc_code varchar(255),         -- Unique procedure code tied to the procedure code
    units varchar(255),             -- Units administered to the patient
    proc_mod1 varchar(255),         -- Procedure modifier 1
    proc_mod2 varchar(255),         -- Procedure modifier 2
    proc_mod3 varchar(255),         -- Procedure modifier 3
    proc_mod4 varchar(255),         -- Procedure modifier 4
    line_charge decimal             -- Amount billed by the provider for this claim-service line
);
ALTER TABLE ub_claims_svc OWNER TO hv;

REVOKE ALL ON TABLE ub_claims_svc FROM PUBLIC;
REVOKE ALL ON TABLE ub_claims_svc FROM hv;
GRANT ALL ON TABLE ub_claims_svc TO hv;

-- UB CLAIMS DIAG

CREATE TABLE ub_claims_diag (
    feed_id varchar(64) not null,    -- Unique data feed identifier
    claim_id integer not null,       -- Unique claim identifier
    hv_id integer not null,          -- Unique patient identifier
    svc_line integer not null,       -- Unique service line number for the claim
    diag_code varchar(255) not null, -- Unique Diagnosis ICD code
    present_admit varchar(255)       -- Is there an extra character on the end of the diags (Y/N/U/W)
);
ALTER TABLE ub_claims_diag OWNER TO hv;

REVOKE ALL ON TABLE ub_claims_diag FROM PUBLIC;
REVOKE ALL ON TABLE ub_claims_diag FROM hv;
GRANT ALL ON TABLE ub_claims_diag TO hv;

-- UB CLAIMS PROC

CREATE TABLE ub_claims_proc (
    feed_id varchar(64) not null,    -- Unique data feed identifier
    claim_id integer not null,       -- Unique claim identifier
    hv_id integer not null,          -- Unique patient identifier
    svc_line integer not null,       -- Unique service line number for the claim
    proc_code varchar(255) not null, -- Unique Procedure code
    svc_date date not null           -- Date on which procedure occurred
);
ALTER TABLE ub_claims_proc OWNER TO hv;

REVOKE ALL ON TABLE ub_claims_proc FROM PUBLIC;
REVOKE ALL ON TABLE ub_claims_proc FROM hv;
GRANT ALL ON TABLE ub_claims_proc TO hv;
