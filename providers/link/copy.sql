
COPY claim_link (hvid, childid, claim_number, zip3, invalid, multi_match, candidates)
FROM :'SOURCE'
CREDENTIALS :'AWS_CREDENTIALS'
FORMAT AS JSON 's3://salusv/redshift/link.jsonpaths'
MAXERROR 20
gzip;

update claim_link set hvid = childid where hvid is null and invalid is False;
