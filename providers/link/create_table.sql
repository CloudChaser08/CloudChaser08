
CREATE TABLE claim_link (
    hvid integer,
    childid integer,
    claim_number varchar distkey,
    zip3 char(3),
    invalid boolean,
    multi_match boolean,
    candidates integer
);
