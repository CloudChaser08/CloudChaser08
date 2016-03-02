
CREATE TABLE claim_link_raw(claim_number varchar distkey, hvid integer);

COPY claim_link_raw (claim_number, hvid)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
GZIP
DELIMITER ','
JSON as 's3://salusv/redshift/link.jsonpaths'

INSERT INTO claim_link (claim_number, provider_id, hvid) SELECT claim_number, :'PROVIDER', hvid FROM claim_link_raw;

DROP TABLE claim_link_raw;
