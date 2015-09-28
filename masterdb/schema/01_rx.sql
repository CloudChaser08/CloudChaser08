
-- RX CLAIMS

CREATE TABLE rx_claims (
    feed_id varchar(64) not null,                -- Unique data feed identifier
    claim_id integer not null,                   -- Unique prescription identifier
    hv_id integer not null,                      -- Unique patient identifier
    rx_number  integer not null,                 -- Prescription identifier assigned by the pharmacy
    date_written date,                           -- Date the prescription was written by the provider
    svc_date date not null,                      -- Date the prescription was picked up by the patient
    payer_id integer,                            -- Unique payer identifier
    bin integer,                                 -- Card issuer or bank ID used for network routing
    pcn integer,                                 -- Processor control number assigned by processor/PBM
    -- XXX validate these types and varchar length
    pharmacy_npi integer not null,               -- Unique NPI for the pharmacy
    prescriber_npi integer not null,             -- Unique NPI for the prescribing provider
    ndc_code varchar(255) not null,              -- Unique NDC code identifying the dispensed product
    fill_number integer,                         -- Indicates whether the prescription is new or a refill
    days_supply integer,                         -- Number of calendar days the fill is expected to cover
    qty_dispensed integer,                       -- Number of units the patient received
    number_refills integer default '0' not null, -- Number of refills permitted by the physician 0= not specified 99= unlimited
    dispensed_as_written integer                 -- Indicates whether the pharmacy needed to change the quantity, supply, etc.
);
ALTER TABLE rx_claims OWNER TO hv;

REVOKE ALL ON TABLE rx_claims FROM PUBLIC;
REVOKE ALL ON TABLE rx_claims FROM hv;
GRANT ALL ON TABLE rx_claims TO hv;

